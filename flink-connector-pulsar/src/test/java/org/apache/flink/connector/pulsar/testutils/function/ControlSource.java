/*
 * Licensed to the Apache Software Foundation (ASF)
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

package org.apache.flink.connector.pulsar.testutils.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.pulsar.client.api.SubscriptionMode.Durable;
import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;

/**
 * This source is used for testing in Pulsar sink. We would generate a fix number of records by the
 * topic name and message index. It wraps a {@link DataGeneratorSource} so the connector test code
 * does not depend on the legacy {@code SourceFunction} API in Flink 2.x.
 */
public class ControlSource {

    private final SharedReference<MessageGenerator> sharedGenerator;
    private final SharedReference<StopSignal> sharedSignal;
    private final int messageCounts;
    private final Duration interval;

    public ControlSource(
            SharedObjectsExtension sharedObjects,
            PulsarRuntimeOperator operator,
            String topic,
            DeliveryGuarantee guarantee,
            int messageCounts,
            Duration interval,
            Duration timeout)
            throws PulsarClientException {
        MessageGenerator generator = new MessageGenerator(topic, guarantee, messageCounts);
        StopSignal signal = new StopSignal(operator, topic, messageCounts, timeout);

        this.sharedGenerator = sharedObjects.add(generator);
        this.sharedSignal = sharedObjects.add(signal);
        this.messageCounts = messageCounts;
        this.interval = interval;
    }

    /** Creates the bounded {@link DataGeneratorSource} that drives this control source. */
    public DataGeneratorSource<String> createSource() {
        double permitsPerSecond = 1000.0 / Math.max(1, interval.toMillis());
        return new DataGeneratorSource<>(
                new MessageGeneratorFunction(sharedGenerator),
                messageCounts,
                RateLimiterStrategy.perSecond(permitsPerSecond),
                TypeInformation.of(String.class));
    }

    public List<String> getExpectedRecords() {
        MessageGenerator generator = sharedGenerator.get();
        return generator.getExpectedRecords();
    }

    public List<String> getConsumedRecords() {
        StopSignal signal = sharedSignal.get();
        return signal.getConsumedRecords();
    }

    /** Bridges the {@link DataGeneratorSource} index into a concrete Pulsar message string. */
    private static final class MessageGeneratorFunction implements GeneratorFunction<Long, String> {

        private static final long serialVersionUID = 1L;

        private final SharedReference<MessageGenerator> sharedGenerator;

        MessageGeneratorFunction(SharedReference<MessageGenerator> sharedGenerator) {
            this.sharedGenerator = sharedGenerator;
        }

        @Override
        public String map(Long index) {
            return sharedGenerator.get().generate(index);
        }
    }

    private static class MessageGenerator {

        private final String topic;
        private final DeliveryGuarantee guarantee;
        private final List<String> expectedRecords;

        public MessageGenerator(String topic, DeliveryGuarantee guarantee, int messageCounts) {
            this.topic = topic;
            this.guarantee = guarantee;
            this.expectedRecords = new ArrayList<>(messageCounts);
        }

        public String generate(long index) {
            String content =
                    guarantee.name() + "-" + topic + "-" + index + "-" + randomAlphanumeric(10);
            expectedRecords.add(content);
            return content;
        }

        public List<String> getExpectedRecords() {
            return expectedRecords;
        }
    }

    /**
     * This is used in {@link ControlSource}, we can stop the source by this method. Make sure you
     * wrap this instance into a {@link SharedReference}.
     */
    private static class StopSignal implements Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(StopSignal.class);

        private final int desiredCounts;
        // This is a thread-safe list.
        private final List<String> consumedRecords;
        private final AtomicLong deadline;
        private final ExecutorService executor;
        private final Consumer<String> consumer;
        private final AtomicReference<PulsarClientException> throwableException;

        public StopSignal(
                PulsarRuntimeOperator operator, String topic, int messageCounts, Duration timeout)
                throws PulsarClientException {
            this.desiredCounts = messageCounts;
            this.consumedRecords = Collections.synchronizedList(new ArrayList<>(messageCounts));
            this.deadline = new AtomicLong(timeout.toMillis() + System.currentTimeMillis());
            this.executor = Executors.newSingleThreadExecutor();
            this.consumer =
                    operator.client()
                            .newConsumer(Schema.STRING)
                            .topic(topic)
                            .subscriptionName(randomAlphanumeric(10))
                            .subscriptionMode(Durable)
                            .subscriptionType(Exclusive)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscribe();
            this.throwableException = new AtomicReference<>();

            // Start consuming.
            executor.execute(
                    () -> {
                        while (consumedRecords.size() < desiredCounts) {
                            // This method would block until we consumed a message.
                            int counts = desiredCounts - consumedRecords.size();
                            for (int i = 0; i < counts; i++) {
                                try {
                                    Message<String> message = consumer.receive();
                                    consumedRecords.add(message.getValue());
                                } catch (PulsarClientException e) {
                                    throwableException.set(e);
                                    break;
                                }
                            }
                        }
                    });
        }

        public boolean canStop() {
            PulsarClientException exception = throwableException.get();
            if (exception != null) {
                LOG.error("Error in consuming messages from Pulsar.");
                LOG.error("", exception);
                return true;
            }

            if (deadline.get() < System.currentTimeMillis()) {
                String errorMsg =
                        String.format(
                                "Timeout for waiting the records from Pulsar. We have consumed %d messages, expect %d messages.",
                                consumedRecords.size(), desiredCounts);
                LOG.warn(errorMsg);
                return true;
            }

            return consumedRecords.size() >= desiredCounts;
        }

        public List<String> getConsumedRecords() {
            return consumedRecords;
        }

        @Override
        public void close() {
            executor.shutdown();
        }
    }
}
