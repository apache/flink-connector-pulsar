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

package org.apache.flink.connector.pulsar.testutils.sink.reader;

import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/** The data reader for a specified topic partition from Pulsar. */
public class PulsarPartitionDataReader<T> implements ExternalSystemDataReader<T>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionDataReader.class);

    private final Consumer<T> consumer;

    public PulsarPartitionDataReader(
            PulsarRuntimeOperator operator, List<String> topics, Schema<T> schema) {
        this(operator, topics, schema, PulsarCrypto.disabled());
    }

    protected PulsarPartitionDataReader(
            PulsarRuntimeOperator operator,
            List<String> topics,
            Schema<T> schema,
            PulsarCrypto pulsarCrypto) {
        // Create the consumer for supporting the E2E tests in the meantime.
        ConsumerBuilder<T> builder =
                operator.client()
                        .newConsumer(schema)
                        .topics(topics)
                        .subscriptionName(randomAlphanumeric(12))
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        CryptoKeyReader cryptoKeyReader = pulsarCrypto.cryptoKeyReader();
        if (cryptoKeyReader != null) {
            // Add the crypto.
            builder.cryptoKeyReader(cryptoKeyReader);
            builder.cryptoFailureAction(ConsumerCryptoFailureAction.FAIL);

            MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto =
                    pulsarCrypto.messageCrypto();
            if (messageCrypto != null) {
                builder.messageCrypto(messageCrypto);
            }
        }

        try {
            this.consumer = builder.subscribe();
        } catch (PulsarClientException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public List<T> poll(Duration timeout) {
        List<T> results = new ArrayList<>();

        while (true) {
            try {
                int millis = Math.toIntExact(timeout.toMillis());
                Message<T> message = consumer.receive(millis, MILLISECONDS);

                if (message != null) {
                    consumer.acknowledgeCumulative(message);
                    results.add(message.getValue());
                } else {
                    break;
                }
            } catch (Exception e) {
                LOG.error("", e);
                break;
            }
        }

        return results;
    }

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
    }
}
