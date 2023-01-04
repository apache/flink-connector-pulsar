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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.util.function.SerializableSupplier;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.api.proto.MessageMetadata;

import java.util.Map;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * A default key reader for the Pulsar client. We would load the pre-generated key file and validate
 * it.
 */
public class PulsarTestKeyReader implements CryptoKeyReader {
    private static final long serialVersionUID = -7488297938196049791L;

    public static final String ENCRYPT_KEY = "flink";

    private final byte[] publicKey =
            ("-----BEGIN PUBLIC KEY-----\n"
                            + "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwZAyz0PSoggZGIoxcrYJ\n"
                            + "L/s9+6aAeuETFmnfT7ityhRgSBLK9MB6lyJKahmGJpEMTgYtZCb2QBlg7w8fOKb3\n"
                            + "Z8UPSbR3DR38cMaftfP37VADFXTW8pxRj6p84NdgeoLDsyiVijIqxpCaPuW6ne4T\n"
                            + "mN1PEV20zbGeWb6l52bWkp1gincc7ghhzzEB8F1Q7YBfRcA7pBRVhmW2yxXK8K/0\n"
                            + "dpkVWK9XQf7/b2I9tLb0OXyaOoMsOCoAqoOy1EmgQ/iF0wO52WN4pcBTshifgrLS\n"
                            + "dAJxGctuHvov0inYzrwxwEw+DWVbTAG8mBRiNkPnIwnewdDTZpMlxNrL+6p2aRDF\n"
                            + "pQIDAQAB\n"
                            + "-----END PUBLIC KEY-----")
                    .getBytes();
    private final byte[] privateKey =
            ("-----BEGIN RSA PRIVATE KEY-----\n"
                            + "MIIEpAIBAAKCAQEAwZAyz0PSoggZGIoxcrYJL/s9+6aAeuETFmnfT7ityhRgSBLK\n"
                            + "9MB6lyJKahmGJpEMTgYtZCb2QBlg7w8fOKb3Z8UPSbR3DR38cMaftfP37VADFXTW\n"
                            + "8pxRj6p84NdgeoLDsyiVijIqxpCaPuW6ne4TmN1PEV20zbGeWb6l52bWkp1gincc\n"
                            + "7ghhzzEB8F1Q7YBfRcA7pBRVhmW2yxXK8K/0dpkVWK9XQf7/b2I9tLb0OXyaOoMs\n"
                            + "OCoAqoOy1EmgQ/iF0wO52WN4pcBTshifgrLSdAJxGctuHvov0inYzrwxwEw+DWVb\n"
                            + "TAG8mBRiNkPnIwnewdDTZpMlxNrL+6p2aRDFpQIDAQABAoIBAQCoOCWwM4VPBDKr\n"
                            + "PQ6UXtfFN1g66A0ovYrVI9XLdviyctrqSErSQqVHy6lYZC5OPiivdnbkX2gLdQLl\n"
                            + "QAMmPRUuvff3WjtMzw6hBD2w6AJD4BGOCCj2WBwZ+1TkIsnaLuLdRRwRKmA6iVlD\n"
                            + "6Gsy7cFiTJN2yDVlvkOcQy/z11ALwe2KbiMg6f72K75fgRoJ1wB54d4+CnBNHOyW\n"
                            + "B5Oi6ahTLp5SOZJuJuXBrvqARdODdYCHKCIEM96YtUhhQG4Ll+erWTwKlz7G848U\n"
                            + "Ac/Yq62S3muuNttBxCdlOMXjh8zcKVMTzt/Cnvc2azYy3Z93R9jppE6BIKb98ohh\n"
                            + "mQfE7E3RAoGBAPSs6wlka78taH/J93wNospuMXiOGqbQCYbwxpQ3228rLzx3ffyZ\n"
                            + "0jw2r1hpVUsmCcaavq8vXJBmq6lSPa74FgAviTYiGPpkio5PWB5aZM4KGld2TyZJ\n"
                            + "480rrEgBYZDQfWFAA68Kx/HXOVfUIccR624/HEXnu8llrLWGmMNARWBvAoGBAMqF\n"
                            + "qxUtiZQM6BzXx2U/uUowqwGUnWegUAaPaLfaOpuRCDcxToff9QLBg5xSaOOOIZk5\n"
                            + "3Fvl4ROnVsz64OLzAcXBGoKxJTwnojpbkw4HMKE7MqD2TWc9D7pc2T5VA650/DDC\n"
                            + "FQNkKg1k/bgH+HCgMnwndRrbM7w9sljjaqca0x0rAoGAQEvof9FZ1yVRnrMuS/Ux\n"
                            + "YEzQx0NgkZF9z24aYPzEt1P718H245hwfM5KCcu0VEksrHohvduOUYwJdDdeakpb\n"
                            + "TbUwM3+GXNZq6rbDC0bp0pMpFO7MId2s9U+SuGFUiD+hkxrFXQxSOqU6NnBSaAO3\n"
                            + "gIMpJN2epXAIkLNMFZMgKBUCgYARIpgkFZNDZIgrEJK9XVPnFBET9CgRQX4j3/Rj\n"
                            + "QeKdkPrZ+KEFXAyV7BufmVVok3kCRuP/HocZq5nrg/qNGTR4L+t3TVeyLERMnbzm\n"
                            + "ffM+YQzak5xe9Mqk4QA8huLl2t4Pngw7Gjl4oqfY70u088jxukDtQcixz6KMZMl8\n"
                            + "VAeyuwKBgQCcbFC00vqMvB56tc96Wdz2vn0tcjyKE+QL8f6KTG/JtVAHSGhfFkmh\n"
                            + "+V+g3/5sr9urOm5I//+riAqbUuuP3IIeVY4UYh5YjgwJcrGRIx0PufExsDaYCl98\n"
                            + "GWeubd7Bk7phXjj36+Sbs3a0dD+HmDdbHAF3JXVZlT7sVUJo4UEzlg==\n"
                            + "-----END RSA PRIVATE KEY-----")
                    .getBytes();

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
        if (ENCRYPT_KEY.equals(keyName)) {
            return new EncryptionKeyInfo(copyKey(publicKey), metadata);
        }

        throw new IllegalArgumentException("We only support encrypt key: \"" + ENCRYPT_KEY + "\"");
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
        if (ENCRYPT_KEY.equals(keyName)) {
            return new EncryptionKeyInfo(copyKey(privateKey), metadata);
        }

        throw new IllegalArgumentException("We only support encrypt key: \"" + ENCRYPT_KEY + "\"");
    }

    private byte[] copyKey(byte[] key) {
        // The byte array is not immutable. Duplicate it for safety.
        byte[] k = new byte[key.length];
        System.arraycopy(key, 0, k, 0, key.length);

        return k;
    }

    /** The implementation for default {@link MessageCryptoBc}. */
    public static class MessageCryptoBcSupplier
            implements SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>> {

        private final boolean producer;

        public MessageCryptoBcSupplier(boolean producer) {
            this.producer = producer;
        }

        @Override
        public MessageCrypto<MessageMetadata, MessageMetadata> get() {
            return new MessageCryptoBc(randomAlphabetic(10), producer);
        }
    }
}
