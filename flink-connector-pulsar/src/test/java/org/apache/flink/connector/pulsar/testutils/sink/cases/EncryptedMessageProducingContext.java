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

package org.apache.flink.connector.pulsar.testutils.sink.cases;

import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader;
import org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.MessageCryptoBcSupplier;
import org.apache.flink.connector.pulsar.testutils.sink.PulsarSinkTestContext;
import org.apache.flink.connector.pulsar.testutils.sink.reader.PulsarEncryptDataReader;
import org.apache.flink.connector.pulsar.testutils.sink.reader.PulsarPartitionDataReader;

import java.util.List;

import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.ENCRYPT_KEY;
import static org.apache.pulsar.client.api.ProducerCryptoFailureAction.FAIL;
import static org.apache.pulsar.client.api.Schema.STRING;

/** Sink the encrypted messages into a topic and try to decrypt them. */
public class EncryptedMessageProducingContext extends PulsarSinkTestContext {

    public EncryptedMessageProducingContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected void setSinkBuilder(PulsarSinkBuilder<String> builder) {
        PulsarCrypto pulsarCrypto =
                PulsarCrypto.builder()
                        .cryptoKeyReader(new PulsarTestKeyReader())
                        .addEncryptKeys(ENCRYPT_KEY)
                        .messageCrypto(new MessageCryptoBcSupplier(true))
                        .build();
        builder.setPulsarCrypto(pulsarCrypto, FAIL);
    }

    @Override
    protected PulsarPartitionDataReader<String> createSinkDataReader(List<String> topics) {
        PulsarCrypto pulsarCrypto =
                PulsarCrypto.builder()
                        .cryptoKeyReader(new PulsarTestKeyReader())
                        .addEncryptKeys(ENCRYPT_KEY)
                        .messageCrypto(new MessageCryptoBcSupplier(false))
                        .build();
        return new PulsarEncryptDataReader<>(operator, topics, STRING, pulsarCrypto);
    }

    @Override
    protected String displayName() {
        return "write messages into one topic by end-to-end encryption";
    }
}
