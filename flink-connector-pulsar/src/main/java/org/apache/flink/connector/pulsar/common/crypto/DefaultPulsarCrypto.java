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

package org.apache.flink.connector.pulsar.common.crypto;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.common.api.proto.MessageMetadata;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation for {@link PulsarCrypto}. */
@PublicEvolving
public class DefaultPulsarCrypto implements PulsarCrypto {

    private final CryptoKeyReader cryptoKeyReader;
    private final Set<String> encryptKeys;
    private final SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>>
            messageCryptoSupplier;

    DefaultPulsarCrypto(
            CryptoKeyReader cryptoKeyReader,
            Set<String> encryptKeys,
            SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>>
                    messageCryptoSupplier) {
        this.cryptoKeyReader = cryptoKeyReader;
        this.encryptKeys = encryptKeys;
        this.messageCryptoSupplier = messageCryptoSupplier;
    }

    @Override
    public CryptoKeyReader cryptoKeyReader() {
        return cryptoKeyReader;
    }

    @Override
    public Set<String> encryptKeys() {
        return encryptKeys;
    }

    @Nullable
    @Override
    public MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto() {
        return messageCryptoSupplier.get();
    }

    /** The builder for building the {@link DefaultPulsarCrypto}. */
    @PublicEvolving
    public static class DefaultPulsarCryptoBuilder {

        private CryptoKeyReader cryptoKeyReader;
        private final Set<String> encryptKeys = new HashSet<>();
        private SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>>
                messageCryptoSupplier = () -> null;

        DefaultPulsarCryptoBuilder() {}

        public DefaultPulsarCryptoBuilder cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
            this.cryptoKeyReader = cryptoKeyReader;
            return this;
        }

        public DefaultPulsarCryptoBuilder addEncryptKeys(String... keys) {
            encryptKeys.addAll(Arrays.asList(keys));
            return this;
        }

        public DefaultPulsarCryptoBuilder messageCrypto(
                SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>>
                        messageCryptoSupplier) {
            this.messageCryptoSupplier = messageCryptoSupplier;
            return this;
        }

        public DefaultPulsarCrypto build() {
            checkNotNull(cryptoKeyReader, "The CryptoKeyReader is required.");
            checkArgument(!encryptKeys.isEmpty(), "The encrypt keys is required.");

            return new DefaultPulsarCrypto(cryptoKeyReader, encryptKeys, messageCryptoSupplier);
        }
    }
}
