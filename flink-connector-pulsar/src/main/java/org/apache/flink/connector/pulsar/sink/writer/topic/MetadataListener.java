/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.sink.writer.topic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isPartition;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/**
 * We need the latest topic metadata for making sure the newly created topic partitions would be
 * used by the Pulsar sink. This routing policy would be different compared with Pulsar Client
 * built-in logic. We use Flink's ProcessingTimer as the executor.
 */
@Internal
public class MetadataListener implements Serializable, Closeable {
    private static final long serialVersionUID = 6186948471557507522L;

    private static final Logger LOG = LoggerFactory.getLogger(MetadataListener.class);

    private final ImmutableList<String> partitions;
    private final ImmutableList<String> topics;
    private ImmutableList<TopicPartition> availablePartitions;

    // Dynamic fields.
    private transient PulsarAdmin pulsarAdmin;
    private transient Long topicMetadataRefreshInterval;
    private transient ProcessingTimeService timeService;
    private transient LoadingCache<String, Optional<Integer>> topicPartitionCache;

    public MetadataListener() {
        this(emptyList());
    }

    public MetadataListener(List<String> topics) {
        ImmutableList.Builder<String> partitionsBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> topicsBuilder = ImmutableList.builder();

        for (String topic : topics) {
            if (isPartition(topic)) {
                partitionsBuilder.add(topic);
            } else {
                topicsBuilder.add(topic);
            }
        }

        this.partitions = partitionsBuilder.build();
        this.topics = topicsBuilder.build();
        this.availablePartitions = ImmutableList.of();
    }

    /** Register the topic metadata update action in process time service. */
    public void open(SinkConfiguration sinkConfiguration, ProcessingTimeService timeService) {
        // Initialize listener properties.
        this.pulsarAdmin = createAdmin(sinkConfiguration);
        this.topicMetadataRefreshInterval = sinkConfiguration.getTopicMetadataRefreshInterval();
        this.timeService = timeService;
        this.topicPartitionCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(topicMetadataRefreshInterval, TimeUnit.MILLISECONDS)
                        .build(
                                new CacheLoader<String, Optional<Integer>>() {
                                    @Override
                                    @ParametersAreNonnullByDefault
                                    public Optional<Integer> load(String topic)
                                            throws PulsarAdminException {
                                        try {
                                            PartitionedTopicMetadata metadata =
                                                    pulsarAdmin
                                                            .topics()
                                                            .getPartitionedTopicMetadata(topic);
                                            return Optional.of(metadata.partitions);
                                        } catch (NotFoundException e) {
                                            return Optional.empty();
                                        }
                                    }
                                });

        // Initialize the topic metadata. Quit if fail to connect to Pulsar.
        try {
            updateTopicMetadata();
        } catch (PulsarAdminException e) {
            throw new FlinkRuntimeException(e);
        }

        // Register time service for update the topic metadata.
        if (topics.isEmpty()) {
            LOG.info("No topics have been provided, skip metadata update timer.");
        } else {
            registerNextTopicMetadataUpdateTimer();
        }
    }

    /**
     * Return all the available topic partitions. We would recalculate the partitions if the topic
     * metadata has been changed. Otherwise, we would return the cached result for better
     * performance.
     */
    public List<TopicPartition> availablePartitions() {
        return availablePartitions;
    }

    /**
     * Query the topic metadata from Pulsar. The query result will be cached in {@link
     * PulsarSinkOptions#PULSAR_TOPIC_METADATA_REFRESH_INTERVAL} interval.
     *
     * @return Return {@link Optional#empty()} if the topic doesn't exist.
     */
    public Optional<TopicMetadata> queryTopicMetadata(String topic) throws PulsarAdminException {
        if (isPartition(topic)) {
            return Optional.of(new TopicMetadata(topic, NON_PARTITIONED));
        }

        try {
            return topicPartitionCache.get(topic).map(size -> new TopicMetadata(topic, size));
        } catch (ExecutionException e) {
            Optional<PulsarAdminException> optional =
                    ExceptionUtils.findThrowable(e, PulsarAdminException.class);
            if (optional.isPresent()) {
                throw optional.get();
            } else {
                throw new FlinkRuntimeException(e);
            }
        }
    }

    @VisibleForTesting
    void refreshTopicMetadata(String topic) {
        topicPartitionCache.refresh(topic);
    }

    @Override
    public void close() throws IOException {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    private void registerNextTopicMetadataUpdateTimer() {
        long currentProcessingTime = timeService.getCurrentProcessingTime();
        long triggerTime = currentProcessingTime + topicMetadataRefreshInterval;

        timeService.registerTimer(triggerTime, time -> triggerNextTopicMetadataUpdate());
    }

    private void triggerNextTopicMetadataUpdate() {
        // Try to update the topic metadata, ignore the pulsar admin exception.
        try {
            updateTopicMetadata();
        } catch (PulsarAdminException e) {
            LOG.warn("", e);
        }

        // Register next timer.
        registerNextTopicMetadataUpdateTimer();
    }

    private void updateTopicMetadata() throws PulsarAdminException {
        ImmutableList.Builder<TopicPartition> parititonsBuilder = ImmutableList.builder();

        for (String topic : topics) {
            Optional<TopicMetadata> optionalMetadata = queryTopicMetadata(topic);
            if (optionalMetadata.isPresent()) {
                TopicMetadata metadata = optionalMetadata.get();
                int partitionSize = metadata.getPartitionSize();
                if (metadata.isPartitioned()) {
                    for (int i = 0; i < partitionSize; i++) {
                        parititonsBuilder.add(new TopicPartition(topic, i));
                    }
                } else {
                    parititonsBuilder.add(new TopicPartition(topic));
                }
            }
        }

        for (String partition : partitions) {
            TopicName topicName = TopicName.get(partition);
            String name = topicName.getPartitionedTopicName();
            int index = topicName.getPartitionIndex();

            parititonsBuilder.add(new TopicPartition(name, index));
        }

        this.availablePartitions = parititonsBuilder.build();
    }
}
