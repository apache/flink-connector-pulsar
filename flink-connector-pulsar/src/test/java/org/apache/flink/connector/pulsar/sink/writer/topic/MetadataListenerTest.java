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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link MetadataListener}. */
class MetadataListenerTest extends PulsarTestSuiteBase {

    @Test
    void listenEmptyTopics() throws Exception {
        MetadataListener listener = new MetadataListener();
        SinkConfiguration configuration = sinkConfiguration(Duration.ofMinutes(5).toMillis());
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        List<TopicPartition> partitions = listener.availablePartitions();
        assertThat(partitions).isEmpty();

        listener.open(configuration, timeService);
        partitions = listener.availablePartitions();
        assertThat(partitions).isEmpty();
    }

    @Test
    void listenOnPartitions() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 6);
        List<TopicPartition> desiredPartitions = topicPartitions(topic, 6);
        List<String> partitionNames =
                desiredPartitions.stream().map(TopicPartition::getFullTopicName).collect(toList());

        MetadataListener listener = new MetadataListener(partitionNames);
        long interval = Duration.ofMinutes(15).toMillis();
        SinkConfiguration configuration = sinkConfiguration(interval);
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        listener.open(configuration, timeService);
        List<TopicPartition> partitions = listener.availablePartitions();
        assertEquals(desiredPartitions, partitions);

        operator().increaseTopicPartitions(topic, 12);
        listener.refreshTopicMetadata(topic);
        timeService.advance(interval);
        partitions = listener.availablePartitions();
        assertEquals(desiredPartitions, partitions);
    }

    @Test
    void fetchTopicPartitionInformation() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        MetadataListener listener = new MetadataListener(singletonList(topic));
        SinkConfiguration configuration = sinkConfiguration(Duration.ofMinutes(10).toMillis());
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        List<TopicPartition> partitions = listener.availablePartitions();
        assertThat(partitions).isEmpty();

        listener.open(configuration, timeService);
        partitions = listener.availablePartitions();

        List<TopicPartition> desiredPartitions = topicPartitions(topic, 8);

        assertThat(partitions).hasSize(8).isEqualTo(desiredPartitions);
    }

    @Test
    void fetchTopicPartitionUpdate() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        long interval = Duration.ofMinutes(20).toMillis();

        MetadataListener listener = new MetadataListener(singletonList(topic));
        SinkConfiguration configuration = sinkConfiguration(interval);
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        timeService.setCurrentTime(System.currentTimeMillis());

        listener.open(configuration, timeService);
        List<TopicPartition> partitions = listener.availablePartitions();
        List<TopicPartition> desiredPartitions = topicPartitions(topic, 8);

        assertThat(partitions).isEqualTo(desiredPartitions);

        // Increase topic partitions and trigger the metadata update logic.
        operator().increaseTopicPartitions(topic, 16);
        listener.refreshTopicMetadata(topic);
        timeService.advance(interval);

        partitions = listener.availablePartitions();
        desiredPartitions = topicPartitions(topic, 16);
        assertThat(partitions).isEqualTo(desiredPartitions);
    }

    @Test
    void fetchNonPartitionTopic() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 0);
        List<TopicPartition> nonPartitionTopic = singletonList(new TopicPartition(topic));

        MetadataListener listener = new MetadataListener(singletonList(topicName(topic)));
        long interval = Duration.ofMinutes(15).toMillis();
        SinkConfiguration configuration = sinkConfiguration(interval);
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        listener.open(configuration, timeService);
        List<TopicPartition> partitions = listener.availablePartitions();
        assertEquals(partitions, nonPartitionTopic);
    }

    private List<TopicPartition> topicPartitions(String topic, int partitionSize) {
        return IntStream.range(0, partitionSize)
                .boxed()
                .map(i -> new TopicPartition(topic, i))
                .collect(toList());
    }

    private SinkConfiguration sinkConfiguration(long interval) {
        Configuration configuration = operator().config();
        configuration.set(PULSAR_TOPIC_METADATA_REFRESH_INTERVAL, interval);

        return new SinkConfiguration(configuration);
    }
}
