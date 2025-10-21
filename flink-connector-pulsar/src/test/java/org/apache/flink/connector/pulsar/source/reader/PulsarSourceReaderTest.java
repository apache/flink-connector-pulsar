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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchemaInitializationContext;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarSchemaWrapper;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_FETCH_ONE_MESSAGE_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.createPartitionSplit;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.createPartitionSplits;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.DEFAULT_PARTITIONS;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION;
import static org.apache.pulsar.shade.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

/** Unit test for {@link PulsarSourceReader}. */
class PulsarSourceReaderTest extends PulsarTestSuiteBase {

    private static final int MAX_EMPTY_POLLING_TIMES = 10;

    @Test
    void assignZeroSplitsCreatesZeroSubscription() throws Exception {
        String topicName = topicName();
        PulsarSourceReader<Integer> reader = sourceReader();

        reader.snapshotState(100L);
        reader.notifyCheckpointComplete(100L);
        // Verify the committed offsets.
        reader.close();
        for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
            verifyNoSubscriptionCreated(TopicNameUtils.topicNameWithPartition(topicName, i));
        }
    }

    @Test
    void assigningEmptySplits() throws Exception {
        String topicName = topicName();
        PulsarSourceReader<Integer> reader = sourceReader();

        final PulsarPartitionSplit emptySplit =
                createPartitionSplit(
                        topicName, 0, Boundedness.CONTINUOUS_UNBOUNDED, MessageId.latest);

        reader.addSplits(singletonList(emptySplit));

        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        reader.close();
    }

    @Test
    void consumeMessagesAndCommitOffsets() throws Exception {
        String topicName = topicName();
        PulsarSourceReader<Integer> reader = sourceReader();

        // set up the partition
        setupSourceReader(reader, topicName, 0, Boundedness.CONTINUOUS_UNBOUNDED);

        // waiting for results
        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        pollUntil(
                reader,
                output,
                () -> output.getEmittedRecords().size() == NUM_RECORDS_PER_PARTITION,
                "The output didn't poll enough records before timeout.");
        reader.snapshotState(100L);
        reader.notifyCheckpointComplete(100L);
        pollUntil(
                reader,
                output,
                reader.cursorsToCommit::isEmpty,
                "The offset commit did not finish before timeout.");

        // verify consumption
        reader.close();
        verifyAllMessageAcknowledged(
                NUM_RECORDS_PER_PARTITION, TopicNameUtils.topicNameWithPartition(topicName, 0));
    }

    @Test
    void offsetCommitOnCheckpointComplete() throws Exception {
        String topicName = topicName();
        PulsarSourceReader<Integer> reader = sourceReader();

        // consume more than 1 partition
        reader.addSplits(
                createPartitionSplits(
                        topicName, DEFAULT_PARTITIONS, Boundedness.CONTINUOUS_UNBOUNDED));
        reader.notifyNoMoreSplits();
        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        long checkpointId = 0;
        int emptyResultTime = 0;
        InputStatus status;
        do {
            checkpointId++;
            status = reader.pollNext(output);
            // Create a checkpoint for each message consumption, but not complete them.
            reader.snapshotState(checkpointId);
            // the first couple of pollNext() might return NOTHING_AVAILABLE before data appears
            if (InputStatus.NOTHING_AVAILABLE == status) {
                emptyResultTime++;
                sleepUninterruptibly(1, TimeUnit.SECONDS);
            }

        } while (emptyResultTime < MAX_EMPTY_POLLING_TIMES
                && status != InputStatus.END_OF_INPUT
                && output.getEmittedRecords().size()
                        < NUM_RECORDS_PER_PARTITION * DEFAULT_PARTITIONS);

        // The completion of the last checkpoint should subsume all previous checkpoints.
        assertThat(reader.cursorsToCommit).hasSize((int) checkpointId);
        long lastCheckpointId = checkpointId;
        // notify checkpoint complete and expect all cursors committed
        assertThatCode(() -> reader.notifyCheckpointComplete(lastCheckpointId))
                .doesNotThrowAnyException();
        assertThat(reader.cursorsToCommit).isEmpty();

        // Verify the committed offsets.
        reader.close();
        for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
            verifyAllMessageAcknowledged(
                    NUM_RECORDS_PER_PARTITION, TopicNameUtils.topicNameWithPartition(topicName, i));
        }
    }

    @ParameterizedTest
    @EnumSource(Boundedness.class)
    @Timeout(600)
    void supportsPausingOrResumingSplits(Boundedness boundedness) throws Exception {
        String topicName = topicName();
        PulsarSourceReader<Integer> reader = sourceReader();

        PulsarPartitionSplit split =
                createPartitionSplit(topicName, 0, boundedness, MessageId.earliest);

        reader.addSplits(singletonList(split));

        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();

        reader.pauseOrResumeSplits(singletonList(split.splitId()), emptyList());

        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.pauseOrResumeSplits(emptyList(), Collections.singleton(split.splitId()));

        do {
            status = reader.pollNext(output);
            Thread.sleep(5);
        } while (status != InputStatus.MORE_AVAILABLE);

        assertThat(status).isEqualTo(InputStatus.MORE_AVAILABLE);

        reader.close();
    }

    private String topicName() throws Exception {
        String topicName = randomAlphabetic(20);
        Random random = new Random(System.currentTimeMillis());
        operator().setupTopic(topicName, Schema.INT32, () -> random.nextInt(20));

        return topicName;
    }

    private PulsarSourceReader<Integer> sourceReader() throws Exception {
        Configuration configuration = operator().config();

        configuration.set(PULSAR_MAX_FETCH_RECORDS, 1);
        configuration.set(PULSAR_FETCH_ONE_MESSAGE_TIME, 2000);
        configuration.set(PULSAR_MAX_FETCH_TIME, 3000L);
        configuration.set(PULSAR_SUBSCRIPTION_NAME, randomAlphabetic(10));

        PulsarDeserializationSchema<Integer> deserializationSchema =
                new PulsarSchemaWrapper<>(Schema.INT32);
        SourceReaderContext context = new TestingReaderContext();
        try {
            deserializationSchema.open(
                    new PulsarDeserializationSchemaInitializationContext(
                            context, operator().client()),
                    new SourceConfiguration(new Configuration()));
        } catch (Exception e) {
            fail("Error while opening deserializationSchema");
        }

        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);

        return PulsarSourceReader.create(
                sourceConfiguration, deserializationSchema, PulsarCrypto.disabled(), null, context);
    }

    private void setupSourceReader(
            PulsarSourceReader<Integer> reader,
            String topicName,
            int partitionId,
            Boundedness boundedness) {
        PulsarPartitionSplit split = createPartitionSplit(topicName, partitionId, boundedness);
        reader.addSplits(singletonList(split));
        reader.notifyNoMoreSplits();
    }

    private void pollUntil(
            PulsarSourceReader<Integer> reader,
            ReaderOutput<Integer> output,
            Supplier<Boolean> condition,
            String errorMessage)
            throws InterruptedException, TimeoutException {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        reader.pollNext(output);
                    } catch (Exception exception) {
                        throw new RuntimeException(
                                "Caught unexpected exception when polling from the reader",
                                exception);
                    }
                    return condition.get();
                },
                Duration.ofSeconds(Integer.MAX_VALUE),
                errorMessage);
    }

    private void verifyAllMessageAcknowledged(int expectedMessages, String partitionName)
            throws PulsarClientException {
        try (Consumer<byte[]> consumer =
                operator()
                        .client()
                        .newConsumer()
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                        .subscriptionName("verify-message")
                        .topic(partitionName)
                        .subscribe()) {
            assertThat(((MessageIdImpl) consumer.getLastMessageId()).getEntryId())
                    .isEqualTo(expectedMessages - 1);
        }
    }

    private void verifyNoSubscriptionCreated(String partitionName) throws PulsarAdminException {
        Map<String, ? extends SubscriptionStats> subscriptionStats =
                operator().admin().topics().getStats(partitionName, true, true).getSubscriptions();
        assertThat(subscriptionStats).isEmpty();
    }
}
