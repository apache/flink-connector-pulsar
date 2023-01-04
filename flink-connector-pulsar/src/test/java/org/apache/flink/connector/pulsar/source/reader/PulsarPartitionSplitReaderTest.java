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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.common.schema.BytesSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.DEFAULT_PARTITIONS;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.metrics.groups.UnregisteredMetricsGroup.createSourceReaderMetricGroup;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link org.apache.flink.connector.pulsar.source.reader.PulsarPartitionSplitReader}.
 */
class PulsarPartitionSplitReaderTest extends PulsarTestSuiteBase {

    @Test
    void pollMessageAfterTimeout() throws InterruptedException, TimeoutException {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        // Add a split
        handleSplit(splitReader, topicName, 0, MessageId.latest);

        // Poll once with a null message
        Message<byte[]> message1 = fetchedMessage(splitReader);
        assertThat(message1).isNull();

        // Send a message to pulsar
        String topic = topicNameWithPartition(topicName, 0);
        operator().sendMessage(topic, STRING, randomAlphabetic(10));

        // Poll this message again
        waitUtil(
                () -> {
                    Message<byte[]> message2 = fetchedMessage(splitReader);
                    return message2 != null;
                },
                ofSeconds(Integer.MAX_VALUE),
                "Couldn't poll message from Pulsar.");
    }

    @Test
    void consumeMessageCreatedAfterHandleSplitChangesAndFetch() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        handleSplit(splitReader, topicName, 0, MessageId.latest);
        operator().sendMessage(topicNameWithPartition(topicName, 0), STRING, randomAlphabetic(10));
        fetchedMessages(splitReader, 1, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChanges() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        seekStartPositionAndHandleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesAndResetToEarliestPosition() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        seekStartPositionAndHandleSplit(splitReader, topicName, 0, MessageId.earliest);
        fetchedMessages(splitReader, NUM_RECORDS_PER_PARTITION, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesAndResetToLatestPosition() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        seekStartPositionAndHandleSplit(splitReader, topicName, 0, MessageId.latest);
        fetchedMessages(splitReader, 0, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseSecondLastMessageIdCursor() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        MessageIdImpl lastMessageId =
                (MessageIdImpl)
                        sneakyAdmin(
                                () ->
                                        operator()
                                                .admin()
                                                .topics()
                                                .getLastMessageId(
                                                        topicNameWithPartition(topicName, 0)));
        // when doing seek directly on consumer, by default it includes the specified messageId
        seekStartPositionAndHandleSplit(
                splitReader,
                topicName,
                0,
                new MessageIdImpl(
                        lastMessageId.getLedgerId(),
                        lastMessageId.getEntryId() - 1,
                        lastMessageId.getPartitionIndex()));
        fetchedMessages(splitReader, 2, true);
    }

    @Test
    void emptyTopic() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().createTopic(topicName, DEFAULT_PARTITIONS);
        seekStartPositionAndHandleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @Test
    void emptyTopicWithoutSeek() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().createTopic(topicName, DEFAULT_PARTITIONS);
        handleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @Test
    void wakeupSplitReaderShouldNotCauseException() {
        PulsarPartitionSplitReader splitReader = splitReader();
        handleSplit(splitReader, "non-exist", 0);

        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                splitReader.fetch();
                            } catch (Throwable e) {
                                error.set(e);
                            }
                        },
                        "testWakeUp-thread");
        t.start();
        long deadline = System.currentTimeMillis() + 5000L;
        while (t.isAlive() && System.currentTimeMillis() < deadline) {
            splitReader.wakeUp();
            sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
        assertThat(error.get()).isNull();
    }

    @Test
    void assignNoSplits() {
        PulsarPartitionSplitReader splitReader = splitReader();
        assertThat(fetchedMessage(splitReader)).isNull();
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesWithoutSeek() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        handleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseLatestStartCursorWithoutSeek() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        handleSplit(splitReader, topicName, 0, MessageId.latest);
        fetchedMessages(splitReader, 0, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseEarliestStartCursorWithoutSeek() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        handleSplit(splitReader, topicName, 0, MessageId.earliest);
        fetchedMessages(splitReader, NUM_RECORDS_PER_PARTITION, true);
    }

    @Test
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseSecondLastMessageWithoutSeek() {
        PulsarPartitionSplitReader splitReader = splitReader();
        String topicName = randomAlphabetic(10);

        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        MessageIdImpl lastMessageId =
                (MessageIdImpl)
                        sneakyAdmin(
                                () ->
                                        operator()
                                                .admin()
                                                .topics()
                                                .getLastMessageId(
                                                        topicNameWithPartition(topicName, 0)));
        // when recover, use exclusive startCursor
        handleSplit(
                splitReader,
                topicName,
                0,
                new MessageIdImpl(
                        lastMessageId.getLedgerId(),
                        lastMessageId.getEntryId() - 1,
                        lastMessageId.getPartitionIndex()));
        fetchedMessages(splitReader, 1, true);
    }

    /** Create a split reader with max message 1, fetch timeout 1s. */
    private PulsarPartitionSplitReader splitReader() {
        return new PulsarPartitionSplitReader(
                operator().client(),
                operator().admin(),
                sourceConfig(),
                new BytesSchema(new PulsarSchema<>(STRING)),
                PulsarCrypto.disabled(),
                createSourceReaderMetricGroup());
    }

    /** Default source config: max message 1, fetch timeout 1s. */
    private SourceConfiguration sourceConfig() {
        Configuration config = operator().config();
        config.set(PULSAR_MAX_FETCH_RECORDS, 1);
        config.set(PULSAR_MAX_FETCH_TIME, 1000L);
        config.set(PULSAR_SUBSCRIPTION_NAME, randomAlphabetic(10));
        config.set(PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, true);

        return new SourceConfiguration(config);
    }

    private void handleSplit(PulsarPartitionSplitReader reader, String topicName, int partitionId) {
        handleSplit(reader, topicName, partitionId, null);
    }

    private void handleSplit(
            PulsarPartitionSplitReader reader,
            String topicName,
            int partitionId,
            MessageId startPosition) {
        TopicPartition partition = new TopicPartition(topicName, partitionId);
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(partition, StopCursor.never(), startPosition, null);
        SplitsAddition<PulsarPartitionSplit> addition = new SplitsAddition<>(singletonList(split));
        reader.handleSplitsChanges(addition);
    }

    private void seekStartPositionAndHandleSplit(
            PulsarPartitionSplitReader reader, String topicName, int partitionId) {
        seekStartPositionAndHandleSplit(reader, topicName, partitionId, MessageId.latest);
    }

    private void seekStartPositionAndHandleSplit(
            PulsarPartitionSplitReader reader,
            String topicName,
            int partitionId,
            MessageId startPosition) {
        TopicPartition partition = new TopicPartition(topicName, partitionId);
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(partition, StopCursor.never(), null, null);
        SplitsAddition<PulsarPartitionSplit> addition = new SplitsAddition<>(singletonList(split));

        // Create the subscription and set the start position for this reader.
        // Remember not to use Consumer.seek(startPosition)
        SourceConfiguration sourceConfiguration = reader.sourceConfiguration;
        PulsarAdmin pulsarAdmin = reader.pulsarAdmin;
        String subscriptionName = sourceConfiguration.getSubscriptionName();
        List<String> subscriptions =
                sneakyAdmin(() -> pulsarAdmin.topics().getSubscriptions(topicName));
        if (!subscriptions.contains(subscriptionName)) {
            // If this subscription is not available. Just create it.
            sneakyAdmin(
                    () ->
                            pulsarAdmin
                                    .topics()
                                    .createSubscription(
                                            topicName, subscriptionName, startPosition));
        } else {
            // Reset the subscription if this is existed.
            sneakyAdmin(
                    () ->
                            pulsarAdmin
                                    .topics()
                                    .resetCursor(topicName, subscriptionName, startPosition));
        }

        // Accept the split and start consuming.
        reader.handleSplitsChanges(addition);
    }

    private Message<byte[]> fetchedMessage(PulsarPartitionSplitReader splitReader) {
        return fetchedMessages(splitReader, 1, false).stream().findFirst().orElse(null);
    }

    private List<Message<byte[]>> fetchedMessages(
            PulsarPartitionSplitReader splitReader, int expectedCount, boolean verify) {
        List<Message<byte[]>> messages = new ArrayList<>(expectedCount);
        List<String> finishedSplits = new ArrayList<>();
        for (int i = 0; i < 3; ) {
            try {
                RecordsWithSplitIds<Message<byte[]>> recordsBySplitIds = splitReader.fetch();
                if (recordsBySplitIds.nextSplit() != null) {
                    // Collect the records in this split.
                    Message<byte[]> record;
                    while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                        messages.add(record);
                    }
                    finishedSplits.addAll(recordsBySplitIds.finishedSplits());
                } else {
                    i++;
                }
            } catch (IOException e) {
                i++;
            }
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        if (verify) {
            assertThat(messages).as("We should fetch the expected size").hasSize(expectedCount);
            assertThat(finishedSplits).as("Split should not be marked as finished").isEmpty();
        }

        return messages;
    }
}
