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

package org.apache.flink.connector.pulsar.testutils.source.writer;

import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.MessageMetadata;

import java.util.List;
import java.util.Set;

import static org.apache.pulsar.client.api.ProducerAccessMode.Shared;

/** Encrypt the messages with the given public key and send the message to Pulsar. */
public class PulsarEncryptDataWriter<T> implements ExternalSystemSplitDataWriter<T> {

    private final Producer<T> producer;

    public PulsarEncryptDataWriter(
            PulsarRuntimeOperator operator,
            String fullTopicName,
            Schema<T> schema,
            PulsarCrypto pulsarCrypto) {
        ProducerBuilder<T> builder =
                operator.client()
                        .newProducer(schema)
                        .topic(fullTopicName)
                        .enableBatching(false)
                        .enableMultiSchema(true)
                        .accessMode(Shared)
                        .cryptoFailureAction(ProducerCryptoFailureAction.FAIL)
                        .cryptoKeyReader(pulsarCrypto.cryptoKeyReader());

        Set<String> encryptKeys = pulsarCrypto.encryptKeys();
        encryptKeys.forEach(builder::addEncryptionKey);

        MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto =
                pulsarCrypto.messageCrypto();
        if (messageCrypto != null) {
            ProducerConfigurationData conf = ((ProducerBuilderImpl<?>) builder).getConf();
            conf.setMessageCrypto(messageCrypto);
        }

        try {
            this.producer = builder.create();
        } catch (PulsarClientException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void writeRecords(List<T> records) {
        try {
            for (T record : records) {
                producer.newMessage().value(record).send();
            }
            producer.flush();
        } catch (PulsarClientException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
