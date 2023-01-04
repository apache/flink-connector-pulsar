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
import org.apache.flink.connector.pulsar.testutils.sink.reader.PulsarEncryptDataReader;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.ENCRYPT_KEY;
import static org.apache.pulsar.client.api.ProducerCryptoFailureAction.FAIL;
import static org.apache.pulsar.client.api.Schema.STRING;

/** The sink context for supporting producing messages which are encrypted. */
public class PulsarEncryptSinkContext extends PulsarSinkTestContext {

    public PulsarEncryptSinkContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected void setSinkBuilder(PulsarSinkBuilder<String> builder) {
        super.setSinkBuilder(builder);

        PulsarCrypto pulsarCrypto =
                PulsarCrypto.builder()
                        .cryptoKeyReader(new PulsarTestKeyReader())
                        .addEncryptKeys(ENCRYPT_KEY)
                        .messageCrypto(new MessageCryptoBcSupplier(true))
                        .build();
        builder.setPulsarCrypto(pulsarCrypto, FAIL);
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        PulsarCrypto pulsarCrypto =
                PulsarCrypto.builder()
                        .cryptoKeyReader(new PulsarTestKeyReader())
                        .addEncryptKeys(ENCRYPT_KEY)
                        .messageCrypto(new MessageCryptoBcSupplier(false))
                        .build();
        PulsarEncryptDataReader<String> reader =
                new PulsarEncryptDataReader<>(operator, topicName, STRING, pulsarCrypto);
        closer.register(reader);

        return reader;
    }

    @Override
    protected String displayName() {
        return "write messages into one topic by End-to-end encryption";
    }
}
