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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicListSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarSubscriber}. */
class PulsarSubscriberTest extends PulsarTestSuiteBase {

    private final String topic1 = topicName("pulsar-subscriber-topic-" + randomAlphanumeric(4));
    private final String topic2 =
            topicName("pulsar-subscriber-pattern-topic-" + randomAlphanumeric(4));
    private final String topic3 = topicName("pulsar-subscriber-topic-2-" + randomAlphanumeric(4));
    private final String topic4 =
            topicName("pulsar-subscriber-non-partitioned-topic-" + randomAlphanumeric(4));
    private final String topic5 =
            topicName("pulsar-subscriber-non-partitioned-topic-2-" + randomAlphanumeric(4));

    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final int NUM_PARALLELISM = 10;

    @BeforeAll
    void setUp() throws Exception {
        operator().createTopic(topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic3, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic4, NON_PARTITIONED);
        operator().createTopic(topic5, NON_PARTITIONED);
    }

    @AfterAll
    void tearDown() throws Exception {
        operator().deleteTopic(topic1);
        operator().deleteTopic(topic2);
        operator().deleteTopic(topic3);
        operator().deleteTopic(topic4);
        operator().deleteTopic(topic5);
    }

    @Test
    void topicListSubscriber() throws Exception {
        PulsarSubscriber subscriber = getTopicListSubscriber(Arrays.asList(topic1, topic2));
        subscriber.open(operator().admin());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);
        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic1, i));
            expectedPartitions.add(new TopicPartition(topic2, i));
        }

        assertEquals(expectedPartitions, topicPartitions);
    }

    @Test
    void subscribeOnePartitionOfMultiplePartitionTopic() throws Exception {
        String partition = topicNameWithPartition(topic1, 2);

        PulsarSubscriber subscriber = getTopicListSubscriber(singletonList(partition));
        subscriber.open(operator().admin());

        Set<TopicPartition> partitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        TopicPartition desiredPartition = new TopicPartition(topic1, 2);
        assertThat(partitions).hasSize(1).containsExactly(desiredPartition);
    }

    @Test
    void subscribeNonPartitionedTopicList() throws Exception {
        PulsarSubscriber subscriber = getTopicListSubscriber(singletonList(topic4));
        subscriber.open(operator().admin());

        Set<TopicPartition> partitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        TopicPartition desiredPartition = new TopicPartition(topic4);
        assertThat(partitions).hasSize(1).containsExactly(desiredPartition);
    }

    @Test
    void subscribeNonPartitionedTopicPattern() throws Exception {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile(
                                "persistent://public/default/pulsar-subscriber-non-partitioned-topic.*?"),
                        AllTopics);
        subscriber.open(operator().admin());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        expectedPartitions.add(new TopicPartition(topic4, -1));
        expectedPartitions.add(new TopicPartition(topic5, -1));

        assertEquals(expectedPartitions, topicPartitions);
    }

    @Test
    void topicPatternSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("persistent://public/default/pulsar-subscriber-topic.*?"),
                        AllTopics);
        subscriber.open(operator().admin());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic1, i));
            expectedPartitions.add(new TopicPartition(topic3, i));
        }

        assertEquals(expectedPartitions, topicPartitions);
    }
}
