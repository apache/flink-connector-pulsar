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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.common.metrics.ProducerMetricsInterceptor;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.shade.com.google.common.base.Strings;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_ACKS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_BYTES_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_MSGS_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_SEND_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.PENDING_QUEUE_SIZE;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.PULSAR_PRODUCER_METRIC_NAME;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_BYTES_RATE;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_LATENCY_MILLIS_50_PCT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_LATENCY_MILLIS_75_PCT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_LATENCY_MILLIS_95_PCT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_LATENCY_MILLIS_999_PCT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_LATENCY_MILLIS_99_PCT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_LATENCY_MILLIS_MAX;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.SEND_MSGS_RATE;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_ACKS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_BYTES_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_MSGS_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_SEND_FAILED;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.createTransaction;
import static org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils.createProducerBuilder;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * All the Pulsar Producers share the same Client, but self hold the queue for a specified topic. So
 * we have to create different instances for different topics.
 */
@Internal
public class ProducerRegister implements Closeable {

    private final PulsarClient pulsarClient;
    private final SinkConfiguration sinkConfiguration;
    private final PulsarCrypto pulsarCrypto;
    private final SinkWriterMetricGroup metricGroup;
    private final Map<TopicPartition, Producer<byte[]>> producers;
    private final Map<TopicPartition, Transaction> transactions;

    public ProducerRegister(
            SinkConfiguration sinkConfiguration,
            PulsarCrypto pulsarCrypto,
            SinkWriterMetricGroup metricGroup) {
        this.pulsarClient = createClient(sinkConfiguration);
        this.sinkConfiguration = sinkConfiguration;
        this.pulsarCrypto = pulsarCrypto;
        this.metricGroup = metricGroup;
        this.producers = new HashMap<>();
        this.transactions = new HashMap<>();

        if (sinkConfiguration.isEnableMetrics()) {
            metricGroup.setCurrentSendTimeGauge(this::currentSendTimeGauge);
        }
    }

    /**
     * Create a TypedMessageBuilder which could be sent to Pulsar directly. First, we would create a
     * topic-related producer or use a cached instead. Then we would try to find a topic-related
     * transaction. We would generate a transaction instance if there is no transaction. Finally, we
     * create the message builder and put the element into it.
     */
    public <T> TypedMessageBuilder<T> createMessageBuilder(
            TopicPartition partition, Schema<T> schema) {
        Producer<byte[]> producer = getOrCreateProducer(partition);
        DeliveryGuarantee deliveryGuarantee = sinkConfiguration.getDeliveryGuarantee();

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            Transaction transaction = getOrCreateTransaction(partition);
            // Pulsar's Producer didn't expose this method.
            return new TypedMessageBuilderImpl<>(
                    (ProducerBase<?>) producer, schema, (TransactionImpl) transaction);
        } else {
            return producer.newMessage(schema);
        }
    }

    /**
     * Convert the transactions into a committable list for Pulsar Committer. The transactions would
     * be removed until Flink triggered a checkpoint.
     */
    public List<PulsarCommittable> prepareCommit() {
        List<PulsarCommittable> committables = new ArrayList<>(transactions.size());
        transactions.forEach(
                (partition, transaction) -> {
                    TxnID txnID = transaction.getTxnID();
                    PulsarCommittable committable =
                            new PulsarCommittable(txnID, partition.getFullTopicName());
                    committables.add(committable);
                });

        clearTransactions();
        return committables;
    }

    /**
     * Flush all the messages buffered in the client and wait until all messages have been
     * successfully persisted.
     */
    public void flush() throws IOException {
        for (Producer<?> producer : producers.values()) {
            producer.flush();
        }
    }

    @Override
    public void close() throws IOException {
        try (Closer closer = Closer.create()) {
            // Flush all the pending messages to Pulsar. This wouldn't cause exception.
            closer.register(this::flush);

            // Abort all the existed transactions.
            closer.register(this::abortTransactions);

            // Remove all the producers.
            closer.register(producers::clear);

            // All the producers would be closed by this method.
            // We would block until all the producers have been successfully closed.
            closer.register(pulsarClient);
        }
    }

    /** Create or return the cached topic-related producer. */
    private Producer<byte[]> getOrCreateProducer(TopicPartition partition) {
        if (producers.containsKey(partition)) {
            return producers.get(partition);
        }

        ProducerBuilder<byte[]> builder = createProducerBuilder(pulsarClient, sinkConfiguration);

        // Set the message crypto key reader.
        CryptoKeyReader cryptoKeyReader = pulsarCrypto.cryptoKeyReader();
        if (cryptoKeyReader != null) {
            builder.cryptoKeyReader(cryptoKeyReader);

            // Set the encrypt keys.
            Set<String> encryptKeys = pulsarCrypto.encryptKeys();
            checkArgument(
                    encryptKeys != null && !encryptKeys.isEmpty(),
                    "You should provide encryptKeys in PulsarCrypto");
            encryptKeys.forEach(builder::addEncryptionKey);

            // Set the message crypto if provided.
            // Pulsar forgets to expose the config in producer builder.
            // See issue https://github.com/apache/pulsar/issues/19139
            MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto =
                    pulsarCrypto.messageCrypto();
            if (messageCrypto != null) {
                ProducerConfigurationData producerConfig =
                        ((ProducerBuilderImpl<?>) builder).getConf();
                producerConfig.setMessageCrypto(messageCrypto);
            }
        }

        // Set the required topic name.
        builder.topic(partition.getFullTopicName());
        // Set the sending counter for metrics.
        builder.intercept(new ProducerMetricsInterceptor(metricGroup));

        Producer<byte[]> producer = sneakyClient(builder::create);

        // Expose the stats for calculating and monitoring.
        exposeProducerMetrics(producer);
        producers.put(partition, producer);

        return producer;
    }

    /**
     * Get the cached topic-related transaction. Or create a new transaction after checkpointing.
     */
    private Transaction getOrCreateTransaction(TopicPartition partition) {
        return transactions.computeIfAbsent(
                partition,
                t -> {
                    long timeoutMillis = sinkConfiguration.getTransactionTimeoutMillis();
                    return createTransaction(pulsarClient, timeoutMillis);
                });
    }

    /** Abort the existed transactions. This method would be used when closing PulsarWriter. */
    private void abortTransactions() {
        if (transactions.isEmpty()) {
            return;
        }

        TransactionCoordinatorClient coordinatorClient =
                ((PulsarClientImpl) pulsarClient).getTcClient();
        // This null check is used for making sure transaction is enabled in client.
        checkNotNull(coordinatorClient);

        try (Closer closer = Closer.create()) {
            for (Transaction transaction : transactions.values()) {
                TxnID txnID = transaction.getTxnID();
                closer.register(() -> coordinatorClient.abort(txnID));
            }

            clearTransactions();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    /**
     * Clean these transactions. All transactions should be passed to Pulsar committer, we would
     * create new transaction when new message comes.
     */
    private void clearTransactions() {
        transactions.clear();
    }

    private Long currentSendTimeGauge() {
        double sendTime =
                producers.values().stream()
                        .map(Producer::getStats)
                        .mapToDouble(ProducerStats::getSendLatencyMillis50pct)
                        .average()
                        .orElse(Long.MAX_VALUE);

        return Math.round(sendTime);
    }

    private void exposeProducerMetrics(Producer<?> producer) {
        if (sinkConfiguration.isEnableMetrics()) {
            String producerIdentity = producer.getProducerName();
            if (Strings.isNullOrEmpty(producerIdentity)) {
                // Fallback to use the topic name.
                producerIdentity = UUID.randomUUID().toString();
            }

            MetricGroup group =
                    metricGroup
                            .addGroup(PULSAR_PRODUCER_METRIC_NAME)
                            .addGroup(producer.getTopic())
                            .addGroup(producerIdentity);
            ProducerStats stats = producer.getStats();

            group.gauge(NUM_MSGS_SENT, stats::getNumMsgsSent);
            group.gauge(NUM_BYTES_SENT, stats::getNumBytesSent);
            group.gauge(NUM_SEND_FAILED, stats::getNumSendFailed);
            group.gauge(NUM_ACKS_RECEIVED, stats::getNumAcksReceived);
            group.gauge(SEND_MSGS_RATE, stats::getSendMsgsRate);
            group.gauge(SEND_BYTES_RATE, stats::getSendBytesRate);
            group.gauge(SEND_LATENCY_MILLIS_50_PCT, stats::getSendLatencyMillis50pct);
            group.gauge(SEND_LATENCY_MILLIS_75_PCT, stats::getSendLatencyMillis75pct);
            group.gauge(SEND_LATENCY_MILLIS_95_PCT, stats::getSendLatencyMillis95pct);
            group.gauge(SEND_LATENCY_MILLIS_99_PCT, stats::getSendLatencyMillis99pct);
            group.gauge(SEND_LATENCY_MILLIS_999_PCT, stats::getSendLatencyMillis999pct);
            group.gauge(SEND_LATENCY_MILLIS_MAX, stats::getSendLatencyMillisMax);
            group.gauge(TOTAL_MSGS_SENT, stats::getTotalMsgsSent);
            group.gauge(TOTAL_BYTES_SENT, stats::getTotalBytesSent);
            group.gauge(TOTAL_SEND_FAILED, stats::getTotalSendFailed);
            group.gauge(TOTAL_ACKS_RECEIVED, stats::getTotalAcksReceived);
            group.gauge(PENDING_QUEUE_SIZE, stats::getPendingQueueSize);
        }
    }
}
