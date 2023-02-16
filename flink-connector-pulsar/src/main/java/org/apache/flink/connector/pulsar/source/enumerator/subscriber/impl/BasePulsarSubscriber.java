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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/** PulsarSubscriber abstract class to simplify Pulsar admin related operations. */
public abstract class BasePulsarSubscriber implements PulsarSubscriber {
    private static final long serialVersionUID = 2053021503331058888L;

    // Pulsar doesn't allow converting a non-partitioned topic into a partitioned topic.
    // So we can just cache all the non-partitioned topics here for speeding up the query time.
    private static final Set<String> NON_PARTITIONED_TOPICS = ConcurrentHashMap.newKeySet();

    protected transient PulsarClient client;
    protected transient PulsarAdmin admin;

    protected TopicMetadata queryTopicMetadata(String topic) throws PulsarAdminException {
        if (NON_PARTITIONED_TOPICS.contains(topic)) {
            return new TopicMetadata(topic, NON_PARTITIONED);
        }

        try {
            PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
            if (metadata.partitions == NON_PARTITIONED) {
                NON_PARTITIONED_TOPICS.add(topic);
            }
            return new TopicMetadata(topic, metadata.partitions);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                // Return null for skipping the topic metadata query.
                return null;
            } else {
                // This method would cause failure for subscribers.
                throw e;
            }
        }
    }

    protected Set<TopicPartition> createTopicPartitions(
            Set<String> topics, RangeGenerator generator, int parallelism)
            throws PulsarAdminException {
        Set<TopicPartition> results = new HashSet<>();

        for (String topic : topics) {
            TopicMetadata metadata = queryTopicMetadata(topic);
            if (metadata != null) {
                List<TopicRange> ranges = generator.range(metadata, parallelism);
                if (!metadata.isPartitioned()) {
                    // For non-partitioned topic.
                    results.add(new TopicPartition(metadata.getName(), ranges));
                } else {
                    // For partitioned topic.
                    for (int i = 0; i < metadata.getPartitionSize(); i++) {
                        results.add(new TopicPartition(metadata.getName(), i, ranges));
                    }
                }
            }
        }

        return results;
    }

    @Override
    public void open(PulsarClient client, PulsarAdmin admin) {
        this.client = client;
        this.admin = admin;
    }
}
