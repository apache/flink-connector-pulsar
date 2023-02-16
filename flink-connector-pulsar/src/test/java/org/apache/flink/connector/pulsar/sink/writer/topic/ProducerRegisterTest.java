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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_VALIDATE_SINK_MESSAGE_BYTES;
import static org.apache.flink.metrics.groups.UnregisteredMetricsGroup.createSinkWriterMetricGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ProducerRegister}. */
class ProducerRegisterTest extends PulsarTestSuiteBase {

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void createMessageBuilderForSendingMessage(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        SinkConfiguration configuration =
                new SinkConfiguration(operator().sinkConfig(deliveryGuarantee));
        ProducerRegister register =
                new ProducerRegister(
                        configuration, PulsarCrypto.disabled(), createSinkWriterMetricGroup());

        String message = randomAlphabetic(10);
        register.createMessageBuilder(topic, Schema.STRING).value(message).send();

        if (deliveryGuarantee == EXACTLY_ONCE) {
            List<PulsarCommittable> committables = register.prepareCommit();
            for (PulsarCommittable committable : committables) {
                TxnID txnID = committable.getTxnID();
                TransactionCoordinatorClient coordinatorClient = operator().coordinatorClient();
                coordinatorClient.commit(txnID);
            }
        }

        Message<String> receiveMessage = operator().receiveMessage(topic, Schema.STRING);
        assertThat(receiveMessage.getValue()).isEqualTo(message);
    }

    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = {"AT_LEAST_ONCE", "NONE"})
    void noneAndAtLeastOnceWouldNotCreateTransaction(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        SinkConfiguration configuration =
                new SinkConfiguration(operator().sinkConfig(deliveryGuarantee));
        ProducerRegister register =
                new ProducerRegister(
                        configuration, PulsarCrypto.disabled(), createSinkWriterMetricGroup());

        String message = randomAlphabetic(10);
        register.createMessageBuilder(topic, Schema.STRING).value(message).sendAsync();

        List<PulsarCommittable> committables = register.prepareCommit();
        assertThat(committables).isEmpty();
    }

    @Test
    void sendMessageBytesWithWrongSchemaAndEnableCheck() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);
        operator().admin().schemas().createSchema(topic, Schema.INT16.getSchemaInfo());

        Configuration configuration = operator().sinkConfig(DeliveryGuarantee.AT_LEAST_ONCE);
        configuration.set(PULSAR_VALIDATE_SINK_MESSAGE_BYTES, true);
        SinkConfiguration sinkConfiguration = new SinkConfiguration(configuration);
        ProducerRegister register =
                new ProducerRegister(
                        sinkConfiguration, PulsarCrypto.disabled(), createSinkWriterMetricGroup());

        long message = ThreadLocalRandom.current().nextLong();
        TypedMessageBuilder<byte[]> builder = register.createMessageBuilder(topic, Schema.BYTES);

        assertThatThrownBy(() -> builder.value(Schema.INT64.encode(message)))
                .isInstanceOf(SchemaSerializationException.class);
    }
}
