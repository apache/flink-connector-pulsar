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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.StopCondition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.MSG_NUM_IN_RECEIVER_QUEUE;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_ACKS_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_ACKS_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_BATCH_RECEIVE_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_BYTES_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_MSGS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_RECEIVE_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.PULSAR_CONSUMER_METRIC_NAME;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.RATE_BYTES_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.RATE_MSGS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_ACKS_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_ACKS_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_BATCH_RECEIVED_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_BYTES_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_MSGS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_RECEIVED_FAILED;
import static org.apache.flink.connector.pulsar.source.config.CursorVerification.FAIL_ON_MISMATCH;
import static org.apache.flink.connector.pulsar.source.config.PulsarSourceConfigUtils.createConsumerBuilder;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.isFullTopicRanges;
import static org.apache.pulsar.client.api.KeySharedPolicy.stickyHashRange;

/**
 * The split reader a given {@link PulsarPartitionSplit}, it would be closed once the {@link
 * PulsarSourceReader} is closed.
 */
@Internal
public class PulsarPartitionSplitReader
        implements SplitReader<Message<byte[]>, PulsarPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionSplitReader.class);

    private final PulsarClient pulsarClient;
    @VisibleForTesting final PulsarAdmin pulsarAdmin;
    @VisibleForTesting final SourceConfiguration sourceConfiguration;
    private final Schema<byte[]> schema;
    private final PulsarCrypto pulsarCrypto;
    private final SourceReaderMetricGroup metricGroup;

    private Consumer<byte[]> pulsarConsumer;
    private PulsarPartitionSplit registeredSplit;

    public PulsarPartitionSplitReader(
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            SourceConfiguration sourceConfiguration,
            Schema<byte[]> schema,
            PulsarCrypto pulsarCrypto,
            SourceReaderMetricGroup metricGroup) {
        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.sourceConfiguration = sourceConfiguration;
        this.schema = schema;
        this.pulsarCrypto = pulsarCrypto;
        this.metricGroup = metricGroup;
    }

    @Override
    @SuppressWarnings("java:S135")
    public RecordsWithSplitIds<Message<byte[]>> fetch() throws IOException {
        RecordsBySplits.Builder<Message<byte[]>> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (pulsarConsumer == null || registeredSplit == null) {
            return builder.build();
        }

        StopCursor stopCursor = registeredSplit.getStopCursor();
        String splitId = registeredSplit.splitId();
        Deadline deadline = Deadline.fromNow(sourceConfiguration.getMaxFetchTime());

        // Consume messages from pulsar until it was woken up by flink reader.
        for (int messageNum = 0;
                messageNum < sourceConfiguration.getMaxFetchRecords() && deadline.hasTimeLeft();
                messageNum++) {
            try {
                int fetchTime = sourceConfiguration.getFetchOneMessageTime();
                if (fetchTime <= 0) {
                    fetchTime = (int) deadline.timeLeftIfAny().toMillis();
                }

                Message<byte[]> message = pulsarConsumer.receive(fetchTime, TimeUnit.MILLISECONDS);
                if (message == null) {
                    break;
                }

                StopCondition condition = stopCursor.shouldStop(message);

                if (condition == StopCondition.CONTINUE || condition == StopCondition.EXACTLY) {
                    // Collect original message.
                    builder.add(splitId, message);
                    LOG.debug("Finished polling message {}", message);
                }

                if (condition == StopCondition.EXACTLY || condition == StopCondition.TERMINATE) {
                    builder.addFinishedSplit(splitId);
                    break;
                }
            } catch (TimeoutException e) {
                break;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PulsarPartitionSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        // Get all the partition assignments and stopping offsets.
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (registeredSplit != null) {
            throw new IllegalStateException("This split reader have assigned split.");
        }

        List<PulsarPartitionSplit> newSplits = splitsChanges.splits();
        Preconditions.checkArgument(
                newSplits.size() == 1, "This pulsar split reader only supports one split.");
        this.registeredSplit = newSplits.get(0);

        // Open stop cursor.
        try {
            registeredSplit.open(pulsarAdmin);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }

        // Reset the start position before creating the consumer.
        MessageId latestConsumedId = registeredSplit.getLatestConsumedId();

        if (latestConsumedId != null) {
            LOG.info("Reset subscription position by the checkpoint {}", latestConsumedId);
            try {
                CursorPosition cursorPosition;
                if (latestConsumedId == MessageId.latest
                        || latestConsumedId == MessageId.earliest) {
                    // for compatibility
                    cursorPosition = new CursorPosition(latestConsumedId, true);
                } else {
                    cursorPosition = new CursorPosition(latestConsumedId, false);
                }

                String topicName = registeredSplit.getPartition().getFullTopicName();
                String subscriptionName = sourceConfiguration.getSubscriptionName();

                // Remove Consumer.seek() here for waiting for pulsar-client-all 2.12.0
                // See https://github.com/apache/pulsar/issues/16757 for more details.

                cursorPosition.seekPosition(pulsarAdmin, topicName, subscriptionName);
            } catch (PulsarAdminException e) {
                if (sourceConfiguration.getVerifyInitialOffsets() == FAIL_ON_MISMATCH) {
                    throw new IllegalArgumentException(e);
                } else {
                    // WARN_ON_MISMATCH would just print this warning message.
                    // No need to print the stacktrace.
                    LOG.warn(
                            "Failed to reset cursor to {} on partition {}",
                            latestConsumedId,
                            registeredSplit.getPartition(),
                            e);
                }
            }
        }

        // Create pulsar consumer.
        try {
            this.pulsarConsumer = createPulsarConsumer(registeredSplit.getPartition());
        } catch (PulsarClientException e) {
            throw new FlinkRuntimeException(e);
        }

        LOG.info("Register split {} consumer for current reader.", registeredSplit);
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<PulsarPartitionSplit> splitsToPause,
            Collection<PulsarPartitionSplit> splitsToResume) {
        // This shouldn't happen but just in case...
        Preconditions.checkState(
                splitsToPause.size() + splitsToResume.size() <= 1,
                "This pulsar split reader only supports one split.");

        if (!splitsToPause.isEmpty()) {
            pulsarConsumer.pause();
        } else if (!splitsToResume.isEmpty()) {
            pulsarConsumer.resume();
        }
    }

    @Override
    public void wakeUp() {
        // Nothing to do on this method.
    }

    @Override
    public void close() throws PulsarClientException {
        if (pulsarConsumer != null) {
            pulsarConsumer.close();
        }
    }

    public void notifyCheckpointComplete(TopicPartition partition, MessageId offsetsToCommit)
            throws PulsarClientException {
        if (pulsarConsumer == null) {
            this.pulsarConsumer = createPulsarConsumer(partition);
        }

        pulsarConsumer.acknowledgeCumulative(offsetsToCommit);
    }

    // --------------------------- Helper Methods -----------------------------

    /** Create a specified {@link Consumer} by the given topic partition. */
    private Consumer<byte[]> createPulsarConsumer(TopicPartition partition)
            throws PulsarClientException {
        ConsumerBuilder<byte[]> consumerBuilder =
                createConsumerBuilder(pulsarClient, schema, sourceConfiguration);

        consumerBuilder.topic(partition.getFullTopicName());

        // Add CryptoKeyReader if it exists for supporting end-to-end encryption.
        CryptoKeyReader cryptoKeyReader = pulsarCrypto.cryptoKeyReader();
        if (cryptoKeyReader != null) {
            consumerBuilder.cryptoKeyReader(cryptoKeyReader);

            // Add MessageCrypto if provided.
            MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto =
                    pulsarCrypto.messageCrypto();
            if (messageCrypto != null) {
                consumerBuilder.messageCrypto(messageCrypto);
            }
        }

        // Add KeySharedPolicy for partial keys subscription.
        if (!isFullTopicRanges(partition.getRanges())) {
            KeySharedPolicy policy = stickyHashRange().ranges(partition.getPulsarRanges());
            // We may enable out of order delivery for speeding up. It was turned off by default.
            policy.setAllowOutOfOrderDelivery(
                    sourceConfiguration.isAllowKeySharedOutOfOrderDelivery());
            consumerBuilder.keySharedPolicy(policy);
        }

        // Create the consumer configuration by using common utils.
        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        // Exposing the consumer metrics.
        exposeConsumerMetrics(consumer);

        return consumer;
    }

    private void exposeConsumerMetrics(Consumer<byte[]> consumer) {
        if (sourceConfiguration.isEnableMetrics()) {
            String consumerIdentity = consumer.getConsumerName();
            if (Strings.isNullOrEmpty(consumerIdentity)) {
                consumerIdentity = UUID.randomUUID().toString();
            }

            MetricGroup group =
                    metricGroup
                            .addGroup(PULSAR_CONSUMER_METRIC_NAME)
                            .addGroup(consumer.getTopic())
                            .addGroup(consumerIdentity);
            ConsumerStats stats = consumer.getStats();

            group.gauge(NUM_MSGS_RECEIVED, stats::getNumMsgsReceived);
            group.gauge(NUM_BYTES_RECEIVED, stats::getNumBytesReceived);
            group.gauge(RATE_MSGS_RECEIVED, stats::getRateMsgsReceived);
            group.gauge(RATE_BYTES_RECEIVED, stats::getRateBytesReceived);
            group.gauge(NUM_ACKS_SENT, stats::getNumAcksSent);
            group.gauge(NUM_ACKS_FAILED, stats::getNumAcksFailed);
            group.gauge(NUM_RECEIVE_FAILED, stats::getNumReceiveFailed);
            group.gauge(NUM_BATCH_RECEIVE_FAILED, stats::getNumBatchReceiveFailed);
            group.gauge(TOTAL_MSGS_RECEIVED, stats::getTotalMsgsReceived);
            group.gauge(TOTAL_BYTES_RECEIVED, stats::getTotalBytesReceived);
            group.gauge(TOTAL_RECEIVED_FAILED, stats::getTotalReceivedFailed);
            group.gauge(TOTAL_BATCH_RECEIVED_FAILED, stats::getTotaBatchReceivedFailed);
            group.gauge(TOTAL_ACKS_SENT, stats::getTotalAcksSent);
            group.gauge(TOTAL_ACKS_FAILED, stats::getTotalAcksFailed);
            group.gauge(MSG_NUM_IN_RECEIVER_QUEUE, stats::getMsgNumInReceiverQueue);
        }
    }
}
