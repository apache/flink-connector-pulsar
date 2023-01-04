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

package org.apache.flink.connector.pulsar.testutils.source.cases;

import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader;
import org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.MessageCryptoBcSupplier;
import org.apache.flink.connector.pulsar.testutils.source.writer.PulsarEncryptDataWriter;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import static org.apache.flink.connector.pulsar.testutils.PulsarTestKeyReader.ENCRYPT_KEY;
import static org.apache.pulsar.client.api.ConsumerCryptoFailureAction.FAIL;

/** We will use this context for producing messages with encryption support. */
public class ConsumeEncryptMessagesContext extends MultipleTopicConsumingContext {

    public ConsumeEncryptMessagesContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected void setSourceBuilder(PulsarSourceBuilder<String> builder) {
        super.setSourceBuilder(builder);

        // Set PulsarCrypto for the Pulsar source.
        PulsarCrypto pulsarCrypto =
                PulsarCrypto.builder()
                        .cryptoKeyReader(new PulsarTestKeyReader())
                        .addEncryptKeys(ENCRYPT_KEY)
                        .messageCrypto(new MessageCryptoBcSupplier(false))
                        .build();
        builder.setPulsarCrypto(pulsarCrypto, FAIL);
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        String partitionName = generatePartitionName();
        PulsarCrypto pulsarCrypto =
                PulsarCrypto.builder()
                        .cryptoKeyReader(new PulsarTestKeyReader())
                        .addEncryptKeys(ENCRYPT_KEY)
                        .messageCrypto(new MessageCryptoBcSupplier(true))
                        .build();
        return new PulsarEncryptDataWriter<>(operator, partitionName, schema, pulsarCrypto);
    }

    @Override
    protected String displayName() {
        return "consume messages by end-to-end encryption";
    }

    @Override
    protected String subscriptionName() {
        return "pulsar-encryption-subscription";
    }
}
