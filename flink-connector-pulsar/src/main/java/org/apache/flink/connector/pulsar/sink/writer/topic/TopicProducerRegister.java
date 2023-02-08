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
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
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
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.com.google.common.base.Strings;

import javax.annotation.Nullable;

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
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.getTcClient;
import static org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils.createProducerBuilder;

/**
 * All the Pulsar Producers share the same Client, but self-hold the queue for a specified topic. So
 * we have to create different instances for different topics.
 */
@Internal
public class TopicProducerRegister implements Closeable {

    private final PulsarClient pulsarClient;
    @Nullable private final TransactionCoordinatorClient coordinatorClient;
    private final SinkConfiguration sinkConfiguration;
    private final PulsarCrypto pulsarCrypto;
    private final SinkWriterMetricGroup metricGroup;
    private final Map<String, Schema<byte[]>> schemas;
    private final Map<String, Map<SchemaHash, Producer<?>>> producers;
    private final Map<String, Transaction> transactions;

    public TopicProducerRegister(
            SinkConfiguration sinkConfiguration,
            PulsarCrypto pulsarCrypto,
            SinkWriterMetricGroup metricGroup) {
        this.pulsarClient = createClient(sinkConfiguration);
        this.sinkConfiguration = sinkConfiguration;
        this.pulsarCrypto = pulsarCrypto;
        this.metricGroup = metricGroup;
        this.schemas = new HashMap<>();
        this.producers = new HashMap<>();
        this.transactions = new HashMap<>();

        if (sinkConfiguration.isEnableMetrics()) {
            metricGroup.setCurrentSendTimeGauge(this::currentSendTimeGauge);
        }

        // Check if we have enabled the transaction in the exactly-once delivery guarantee.
        if (sinkConfiguration.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            this.coordinatorClient = getTcClient(pulsarClient);
        } else {
            this.coordinatorClient = null;
        }
    }

    /**
     * Create a TypedMessageBuilder which could be sent to Pulsar directly. First, we would create a
     * topic-related producer or use a cached one instead. Then we would try to find a topic-related
     * transaction. We would generate a transaction instance if there is no transaction. Finally, we
     * created the message builder and put the element into it.
     *
     * <p>Pulsar's Producer doesn't have {@code producer.newMessage(schema, transaction)} method. We
     * have to manually create it.
     */
    @SuppressWarnings("unchecked")
    public <T> TypedMessageBuilder<T> createMessageBuilder(
            String topic, @Nullable Schema<?> schema) {
        if (schema == null || schema.getSchemaInfo().getType() == SchemaType.BYTES) {
            schema = getBytesSchema(topic);
        }
        ProducerBase<?> producer = (ProducerBase<?>) getOrCreateProducer(topic, schema);
        TransactionImpl transaction = null;

        if (sinkConfiguration.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            transaction = (TransactionImpl) getOrCreateTransaction(topic);
        }

        return (TypedMessageBuilder<T>)
                new TypedMessageBuilderImpl<>(producer, schema, transaction);
    }

    /**
     * Convert the transactions into a committable list for Pulsar Committer. The transactions would
     * be removed until Flink triggered a checkpoint.
     */
    public List<PulsarCommittable> prepareCommit() {
        List<PulsarCommittable> committables = new ArrayList<>(transactions.size());
        for (Map.Entry<String, Transaction> entry : transactions.entrySet()) {
            String topic = entry.getKey();
            Transaction transaction = entry.getValue();
            TxnID txnID = transaction.getTxnID();

            committables.add(new PulsarCommittable(txnID, topic));
        }
        transactions.clear();

        return committables;
    }

    /**
     * Flush all the messages buffered in the client and wait until all messages have been
     * successfully persisted.
     */
    public void flush() throws IOException {
        for (Map<SchemaHash, Producer<?>> set : producers.values()) {
            for (Producer<?> producer : set.values()) {
                producer.flush();
            }
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

            // This method would close all the producers.
            // We would block until all the producers have been successfully closed.
            closer.register(pulsarClient);
        }
    }

    /** Create or return the cached topic-related producer. */
    @SuppressWarnings("unchecked")
    private <T> Producer<T> getOrCreateProducer(String topic, Schema<T> schema) {
        Map<SchemaHash, Producer<?>> set = producers.computeIfAbsent(topic, t -> new HashMap<>());
        SchemaHash hash = PulsarSchemaUtils.hash(schema);
        if (set.containsKey(hash)) {
            return (Producer<T>) set.get(hash);
        }

        ProducerBuilder<T> builder = createProducerBuilder(pulsarClient, schema, sinkConfiguration);

        // Enable end-to-end encryption if provided.
        configPulsarCrypto(builder);

        // Set the required topic name.
        builder.topic(topic);
        // Set the sending counter for metrics.
        builder.intercept(new ProducerMetricsInterceptor(metricGroup));

        Producer<T> producer = sneakyClient(builder::create);

        // Expose the stats for calculating and monitoring.
        exposeProducerMetrics(producer);
        set.put(hash, producer);

        return producer;
    }

    private void configPulsarCrypto(ProducerBuilder<?> builder) {
        CryptoKeyReader cryptoKeyReader = pulsarCrypto.cryptoKeyReader();
        if (cryptoKeyReader == null) {
            return;
        }

        // Set the message crypto key reader.
        builder.cryptoKeyReader(cryptoKeyReader);

        // Set the encrypt keys.
        Set<String> encryptKeys = pulsarCrypto.encryptKeys();
        if (encryptKeys == null || encryptKeys.isEmpty()) {
            throw new IllegalArgumentException("You should provide encryptKeys in PulsarCrypto");
        }
        encryptKeys.forEach(builder::addEncryptionKey);

        // Set the message crypto if provided.
        // Pulsar forgets to expose the config in producer builder.
        // See issue https://github.com/apache/pulsar/issues/19139
        MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto =
                pulsarCrypto.messageCrypto();
        if (messageCrypto != null) {
            ProducerConfigurationData producerConfig = ((ProducerBuilderImpl<?>) builder).getConf();
            producerConfig.setMessageCrypto(messageCrypto);
        }
    }

    /**
     * Get the cached topic-related transaction. Or create a new transaction after checkpointing.
     */
    private Transaction getOrCreateTransaction(String topic) {
        return transactions.computeIfAbsent(
                topic,
                t -> {
                    long timeoutMillis = sinkConfiguration.getTransactionTimeoutMillis();
                    return createTransaction(pulsarClient, timeoutMillis);
                });
    }

    /**
     * {@link Schema#AUTO_PRODUCE_BYTES} is used for extra validation. But it should be initialized
     * with extra info in the Pulsar client. So it can't be reused and will be cached here.
     */
    private Schema<byte[]> getBytesSchema(String topic) {
        if (sinkConfiguration.isValidateSinkMessageBytes()) {
            return schemas.computeIfAbsent(topic, t -> Schema.AUTO_PRODUCE_BYTES());
        } else {
            return Schema.BYTES;
        }
    }

    /** Abort the existed transactions. This method would be used when closing PulsarWriter. */
    private void abortTransactions() {
        if (coordinatorClient == null || transactions.isEmpty()) {
            return;
        }

        try (Closer closer = Closer.create()) {
            for (Transaction transaction : transactions.values()) {
                TxnID txnID = transaction.getTxnID();
                closer.register(() -> coordinatorClient.abort(txnID));
            }

            transactions.clear();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private Long currentSendTimeGauge() {
        double sendTime =
                producers.values().stream()
                        .flatMap(set -> set.values().stream())
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
