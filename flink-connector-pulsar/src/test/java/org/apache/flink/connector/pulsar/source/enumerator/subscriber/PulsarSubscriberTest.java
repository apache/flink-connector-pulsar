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

import org.apache.pulsar.common.naming.TopicName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
import static org.apache.pulsar.client.api.RegexSubscriptionMode.NonPersistentOnly;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.PersistentOnly;
import static org.apache.pulsar.common.naming.TopicDomain.non_persistent;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PulsarSubscriber}. */
class PulsarSubscriberTest extends PulsarTestSuiteBase {

    private final String topic1 =
            topicName("flink/regex/pulsar-subscriber-topic-" + randomAlphanumeric(4));
    private final String topic2 =
            topicName("flink/regex/pulsar-subscriber-pattern-topic-" + randomAlphanumeric(4));
    private final String topic3 =
            topicName("flink/regex/pulsar-subscriber-topic-2-" + randomAlphanumeric(4));
    private final String topic4 =
            topicName(
                    "flink/regex/pulsar-subscriber-non-partitioned-topic-" + randomAlphanumeric(4));
    private final String topic5 =
            topicName(
                    "flink/regex/pulsar-subscriber-non-partitioned-topic-2-"
                            + randomAlphanumeric(4));
    private final String topic6 =
            topicName("pulsar-subscriber-simple-topic-" + randomAlphanumeric(4));
    private final String topic7 =
            TopicName.get(
                            non_persistent.value(),
                            "public",
                            "default",
                            "pulsar-subscriber-simple-topic-2-" + randomAlphanumeric(4))
                    .toString();

    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final int NUM_PARALLELISM = 10;

    @BeforeAll
    void setUp() throws Exception {
        operator().createNamespace("flink/regex");

        operator().createTopic(topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic3, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic4, NON_PARTITIONED);
        operator().createTopic(topic5, NON_PARTITIONED);
        operator().createTopic(topic6, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic7, NUM_PARTITIONS_PER_TOPIC);
    }

    @AfterAll
    void tearDown() throws Exception {
        operator().deleteTopic(topic1);
        operator().deleteTopic(topic2);
        operator().deleteTopic(topic3);
        operator().deleteTopic(topic4);
        operator().deleteTopic(topic5);
        operator().deleteTopic(topic6);
    }

    @Test
    void topicListSubscriber() throws Exception {
        PulsarSubscriber subscriber = getTopicListSubscriber(Arrays.asList(topic1, topic2));
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);
        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic1, i));
            expectedPartitions.add(new TopicPartition(topic2, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void subscribeOnePartitionOfMultiplePartitionTopic() throws Exception {
        String partition = topicNameWithPartition(topic1, 2);

        PulsarSubscriber subscriber = getTopicListSubscriber(singletonList(partition));
        subscriber.open(operator().client());

        Set<TopicPartition> partitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        TopicPartition desiredPartition = new TopicPartition(topic1, 2);
        assertThat(partitions).hasSize(1).containsExactly(desiredPartition);
    }

    @Test
    void subscribeNonPartitionedTopicList() throws Exception {
        PulsarSubscriber subscriber = getTopicListSubscriber(singletonList(topic4));
        subscriber.open(operator().client());

        Set<TopicPartition> partitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        TopicPartition desiredPartition = new TopicPartition(topic4);
        assertThat(partitions).hasSize(1).containsExactly(desiredPartition);
    }

    @Test
    void subscribeNonPartitionedTopicPattern() throws Exception {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("flink/regex/pulsar-subscriber-non-partitioned-topic-.*"),
                        AllTopics);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        expectedPartitions.add(new TopicPartition(topic4, -1));
        expectedPartitions.add(new TopicPartition(topic5, -1));

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void topicPatternSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("flink/regex/pulsar-subscriber-topic-.*"), AllTopics);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic1, i));
            expectedPartitions.add(new TopicPartition(topic3, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void simpleTopicPatternSubscriber() throws Exception {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("pulsar-subscriber-simple-topic-.*"), PersistentOnly);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic6, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    @Disabled("Disable for FLINK-31107")
    void regexSubscriptionModeFilterForNonPersistentTopics() throws Exception {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("pulsar-subscriber-simple-topic-.*"), NonPersistentOnly);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic7, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }
}
