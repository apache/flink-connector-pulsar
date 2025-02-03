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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.sink.callback.SinkUserCallback;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContextImpl;
import org.apache.flink.connector.pulsar.sink.writer.delayer.MessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.MetadataListener;
import org.apache.flink.connector.pulsar.sink.writer.topic.ProducerRegister;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.shade.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible to write records in a Pulsar topic and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
@Internal
public class PulsarWriter<IN> implements PrecommittingSinkWriter<IN, PulsarCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarWriter.class);

    private final PulsarSerializationSchema<IN> serializationSchema;
    private final MetadataListener metadataListener;
    private final TopicRouter<IN> topicRouter;
    private final MessageDelayer<IN> messageDelayer;
    private final DeliveryGuarantee deliveryGuarantee;
    private final PulsarSinkContext sinkContext;
    private final ProducerRegister producerRegister;
    private final MailboxExecutor mailboxExecutor;
    private final AtomicLong pendingMessages;

    private final SinkUserCallback<IN> userCallback;

    /**
     * Constructor creating a Pulsar writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * PulsarSerializationSchema#open(InitializationContext, PulsarSinkContext, SinkConfiguration)}
     * fails.
     *
     * @param sinkConfiguration The configuration to configure the Pulsar producer.
     * @param serializationSchema Transform the incoming records into different message properties.
     * @param metadataListener The listener for querying topic metadata.
     * @param topicRouter Topic router to choose the topic by incoming records.
     * @param messageDelayer Used to delay sending messages downstream in {@link SubscriptionType#Shared} subscription
     * @param pulsarCrypto Used for end-to-end encryption.
     * @param initContext Context to provide information about the runtime environment.
     * @param userCallback The callback to, optionally, trigger when the writer takes action.
     */
    public PulsarWriter(
            SinkConfiguration sinkConfiguration,
            PulsarSerializationSchema<IN> serializationSchema,
            MetadataListener metadataListener,
            TopicRouter<IN> topicRouter,
            MessageDelayer<IN> messageDelayer,
            PulsarCrypto pulsarCrypto,
            InitContext initContext,
            @Nullable SinkUserCallback<IN> userCallback)
            throws PulsarClientException {
        checkNotNull(sinkConfiguration);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.metadataListener = checkNotNull(metadataListener);
        this.topicRouter = checkNotNull(topicRouter);
        this.messageDelayer = checkNotNull(messageDelayer);
        checkNotNull(initContext);

        this.deliveryGuarantee = sinkConfiguration.getDeliveryGuarantee();
        this.sinkContext =
                new PulsarSinkContextImpl(initContext, sinkConfiguration, metadataListener);

        // Initialize topic metadata listener.
        LOG.debug("Initialize topic metadata after creating Pulsar writer.");
        ProcessingTimeService timeService = initContext.getProcessingTimeService();
        this.metadataListener.open(sinkConfiguration, timeService);

        // Initialize topic router.
        this.topicRouter.open(sinkConfiguration);

        // Initialize the serialization schema.
        try {
            InitializationContext initializationContext =
                    initContext.asSerializationSchemaInitializationContext();
            this.serializationSchema.open(initializationContext, sinkContext, sinkConfiguration);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.userCallback = userCallback;

        // Create this producer register after opening serialization schema!
        SinkWriterMetricGroup metricGroup = initContext.metricGroup();
        this.producerRegister = new ProducerRegister(sinkConfiguration, pulsarCrypto, metricGroup);
        this.mailboxExecutor = initContext.getMailboxExecutor();
        this.pendingMessages = new AtomicLong(0);
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        PulsarMessage<?> message = serializationSchema.serialize(element, sinkContext);

        // Choose the right topic to send.
        String key = message.getKey();
        List<TopicPartition> partitions = metadataListener.availablePartitions();
        TopicPartition partition = topicRouter.route(element, key, partitions, sinkContext);
        String topic = partition.getFullTopicName();

        // Create message builder for sending messages.
        TypedMessageBuilder<?> builder = createMessageBuilder(topic, context, message);

        // Message Delay delivery.
        long deliverAt = messageDelayer.deliverAt(element, sinkContext);
        if (deliverAt > 0) {
            builder.deliverAt(deliverAt);
        }

        // invoke user callback before send
        invokeUserCallbackBeforeSend(element, message, topic);

        // Perform message sending.
        if (deliveryGuarantee == DeliveryGuarantee.NONE) {
            // We would just ignore the sending exception. This may cause data loss.
            CompletableFuture<MessageId> future  = builder.sendAsync();
            future.whenComplete(
                    (id, ex) -> {
                        invokeUserCallbackAfterSend(element, message, topic, id, ex);
                    });
        } else {
            // Increase the pending message count.
            pendingMessages.incrementAndGet();
            CompletableFuture<MessageId> future = builder.sendAsync();
            future.whenComplete(
                    (id, ex) -> {
                        pendingMessages.decrementAndGet();
                        if (ex != null) {
                            mailboxExecutor.execute(
                                    () -> throwSendingException(topic, ex),
                                    "Failed to send data to Pulsar");
                        } else {
                            LOG.debug("Sent message to Pulsar {} with message id {}", topic, id);
                        }
                        invokeUserCallbackAfterSend(element, message, topic, id, ex);
                    });
        }
    }

    private void callSafely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.warn("Exception from user callback", t);
        }
    }

    private void invokeUserCallbackBeforeSend(IN element, PulsarMessage<?> message, String topic) {
        if (userCallback == null) {
            return;
        }

        callSafely(() -> userCallback.beforeSend(element, message, topic));
    }

    private void invokeUserCallbackAfterSend(
            IN element,
            PulsarMessage<?> message,
            String topic,
            MessageId messageId,
            Throwable exception) {
        if (userCallback == null) {
            return;
        }

        if (exception == null) {
            callSafely(() -> userCallback.onSendSucceeded(element, message, topic, messageId));
        } else {
            callSafely(() -> userCallback.onSendFailed(element, message, topic, exception));
        }
    }

    private void throwSendingException(String topic, Throwable ex) {
        throw new FlinkRuntimeException("Failed to send data to Pulsar: " + topic, ex);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private TypedMessageBuilder<?> createMessageBuilder(
            String topic, Context context, PulsarMessage<?> message) throws PulsarClientException {

        Schema<?> schema = message.getSchema();
        TypedMessageBuilder<?> builder = producerRegister.createMessageBuilder(topic, schema);

        byte[] orderingKey = message.getOrderingKey();
        if (orderingKey != null && orderingKey.length > 0) {
            builder.orderingKey(orderingKey);
        }

        String key = message.getKey();
        if (!Strings.isNullOrEmpty(key)) {
            builder.key(key);
        }

        if (message.isBase64EncodedKey()) {
            // HACK - otherwise we should hold both keys and keyBytes fields which
            // is more confusing.
            ((TypedMessageBuilderImpl<?>) builder)
                    .getMetadataBuilder()
                    .setPartitionKeyB64Encoded(true);
        }

        long eventTime = message.getEventTime();
        if (eventTime > 0) {
            builder.eventTime(eventTime);
        } else {
            // Set default message timestamp if flink has provided one.
            Long timestamp = context.timestamp();
            if (timestamp != null && timestamp > 0L) {
                builder.eventTime(timestamp);
            }
        }

        // Schema evolution would serialize the message by Pulsar Schema in TypedMessageBuilder.
        // The type has been checked in PulsarMessageBuilder#value.
        Object value = message.getValue();
        if (value == null) {
            LOG.warn("Send a message with empty payloads, this is a tombstone message in Pulsar.");
        }
        ((TypedMessageBuilder) builder).value(value);

        Map<String, String> properties = message.getProperties();
        if (properties != null && !properties.isEmpty()) {
            builder.properties(properties);
        }

        Long sequenceId = message.getSequenceId();
        if (sequenceId != null) {
            builder.sequenceId(sequenceId);
        }

        List<String> clusters = message.getReplicationClusters();
        if (clusters != null && !clusters.isEmpty()) {
            builder.replicationClusters(clusters);
        }

        if (message.isDisableReplication()) {
            builder.disableReplication();
        }

        return builder;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (endOfInput || deliveryGuarantee != DeliveryGuarantee.NONE) {
            LOG.info("Flush the pending messages to Pulsar.");

            // Try to flush pending messages.
            producerRegister.flush();
            // Make sure all the pending messages should be flushed to Pulsar.
            while (pendingMessages.longValue() > 0) {
                producerRegister.flush();
            }
        }
    }

    @Override
    public Collection<PulsarCommittable> prepareCommit() {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            return producerRegister.prepareCommit();
        } else {
            return emptyList();
        }
    }

    @Override
    public void close() throws Exception {
        // Close all the resources and throw the exception at last.
        closeAll(metadataListener, producerRegister, userCallback);
    }
}
