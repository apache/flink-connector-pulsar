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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.common.crypto.DefaultPulsarCrypto.DefaultPulsarCryptoBuilder;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.common.api.proto.MessageMetadata;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Use it for end-to-end encryption support in Flink. You should provide at least a {@link
 * CryptoKeyReader} and the encryption keys.
 */
@PublicEvolving
public interface PulsarCrypto extends Serializable {

    /**
     * Crypto is a key store for encrypting. The key pair is stored with a key name. Pulsar will
     * randomly choose a key name from the {@link #encryptKeys()} and get the related public key or
     * private key for encrypting or decrypting the message.
     *
     * @return A non-null instance will enable the end-to-end encryption.
     */
    @Nullable
    CryptoKeyReader cryptoKeyReader();

    /**
     * Return a set of key names. These key names can be used to acquire the key pairs in {@link
     * CryptoKeyReader}.
     *
     * <p>At the time of producer creation, the Pulsar client checks if there are keys added to
     * encryptionKeys. If keys are found, a callback {@link CryptoKeyReader#getPrivateKey(String,
     * Map)} and {@link CryptoKeyReader#getPublicKey(String, Map)} is invoked against each key to
     * load the values of the key. Application should implement this callback to return the key in
     * pkcs8 format. If compression is enabled, the message is encrypted after compression. If batch
     * messaging is enabled, the batched message is encrypted.
     *
     * @return It shouldn't be a null or empty instance if your have returned a key reader.
     */
    Set<String> encryptKeys();

    /**
     * {@link MessageCrypto} is used to define how to decrypt/encrypt the message. It's not required
     * by default, because Pulsar will provide a default implementation by using the bouncy castle.
     */
    @Nullable
    default MessageCrypto<MessageMetadata, MessageMetadata> messageCrypto() {
        return null;
    }

    /** Disable the end-to-end encryption. */
    @Internal
    static PulsarCrypto disabled() {
        return new PulsarCryptoDisabled();
    }

    /** Return the builder for building a {@link PulsarCrypto} instance. */
    static DefaultPulsarCryptoBuilder builder() {
        return new DefaultPulsarCryptoBuilder();
    }
}
