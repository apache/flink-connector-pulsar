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

package org.apache.flink.connector.pulsar.sink.committer;

import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for serializing and deserializing {@link PulsarCommittable} with {@link
 * PulsarCommittableSerializer}.
 */
class PulsarCommittableSerializerTest {

    private static final PulsarCommittableSerializer INSTANCE = new PulsarCommittableSerializer();

    @Test
    void v2SerializeAndDeserializeWithMessages() throws IOException {
        TxnID txnID =
                new TxnID(
                        ThreadLocalRandom.current().nextLong(),
                        ThreadLocalRandom.current().nextLong());
        Map<String, MessageIdPojo> messages = new HashMap<>();
        messages.put("topic-a", new MessageIdPojo(1L, 2L, 3, 0, -1));
        messages.put("topic-b", new MessageIdPojo(10L, 20L, 1, 0, -1));

        PulsarCommittable committable =
                new PulsarCommittable(txnID, messages, PulsarCommittableSerializer.CURRENT_VERSION);

        byte[] bytes = INSTANCE.serialize(committable);
        PulsarCommittable committable1 = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertThat(committable1).isEqualTo(committable);
        assertThat(committable1.getVersion())
                .isEqualTo(PulsarCommittableSerializer.CURRENT_VERSION);
    }

    @Test
    void v2SerializeAndDeserializeWithoutMessages() throws IOException {
        PulsarCommittable committable =
                new PulsarCommittable(
                        new TxnID(1L, 2L),
                        Collections.emptyMap(),
                        PulsarCommittableSerializer.CURRENT_VERSION);

        byte[] bytes = INSTANCE.serialize(committable);
        PulsarCommittable deserialized = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(committable);
        assertThat(deserialized.getLatestPublishedMessages()).isEmpty();
        assertThat(deserialized.getVersion())
                .isEqualTo(PulsarCommittableSerializer.CURRENT_VERSION);
    }

    @Test
    void v1DeserializationIsCompatible() throws IOException {
        TxnID txnID =
                new TxnID(
                        ThreadLocalRandom.current().nextLong(),
                        ThreadLocalRandom.current().nextLong());
        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(txnID.getMostSigBits());
            output.writeLong(txnID.getLeastSigBits());
            output.writeUTF("Topic_Placeholder");
            output.flush();
            serialized = bytes.toByteArray();
        }

        PulsarCommittable committable = INSTANCE.deserialize(1, serialized);

        assertThat(committable.getTxnID()).isEqualTo(txnID);
        assertThat(committable.getLatestPublishedMessages()).isEmpty();
        assertThat(committable.getVersion()).isEqualTo(1);

        PulsarCommittable roundTripped = committable;
        for (int snapshot = 0; snapshot < 3; snapshot++) {
            roundTripped =
                    INSTANCE.deserialize(INSTANCE.getVersion(), INSTANCE.serialize(roundTripped));
            assertThat(roundTripped).isEqualTo(committable);
            assertThat(roundTripped.getVersion()).isEqualTo(1);
        }
    }

    @Test
    void v2StateIsReadableByV1Deserializer() throws IOException {
        TxnID txnID = new TxnID(1L, 2L);
        byte[] serialized =
                INSTANCE.serialize(
                        new PulsarCommittable(
                                txnID,
                                Collections.singletonMap("topic", messageId()),
                                PulsarCommittableSerializer.CURRENT_VERSION));

        // Connector versions that only know V1 ignore the serializer version and trailing bytes.
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            assertThat(new TxnID(input.readLong(), input.readLong())).isEqualTo(txnID);
            assertThat(input.readUTF()).isEqualTo("unknown_due_to_v2_serialization");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void originMarkerPreservesV2FieldLayout(int originVersion) throws IOException {
        byte[] serialized =
                INSTANCE.serialize(
                        new PulsarCommittable(
                                new TxnID(1L, 2L), Collections.emptyMap(), originVersion));

        assertThat(INSTANCE.getVersion()).isEqualTo(2);
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
            assertThat(input.readLong()).isEqualTo(1L);
            assertThat(input.readLong()).isEqualTo(2L);
            assertThat(input.readUTF())
                    .isEqualTo(
                            originVersion == 1
                                    ? "restored_from_v1"
                                    : "unknown_due_to_v2_serialization");
            assertThat(input.readInt()).isZero();
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 3})
    void unknownVersionFailsDeserialization(int version) {
        assertThatThrownBy(() -> INSTANCE.deserialize(version, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Pulsar committable version");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 3})
    void unknownOriginVersionFailsSerialization(int version) {
        PulsarCommittable committable =
                new PulsarCommittable(new TxnID(1L, 2L), Collections.emptyMap(), version);

        assertThatThrownBy(() -> INSTANCE.serialize(committable))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Pulsar committable version");
    }

    @Test
    void v1OriginWithMessagesFailsSerialization() {
        PulsarCommittable committable =
                new PulsarCommittable(
                        new TxnID(1L, 2L), Collections.singletonMap("topic", messageId()), 1);

        assertThatThrownBy(() -> INSTANCE.serialize(committable))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("V1 committable must not contain published message IDs");
    }

    @Test
    void v1OriginMarkerWithMessagesFailsDeserialization() throws IOException {
        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(1L);
            output.writeLong(2L);
            output.writeUTF(PulsarCommittableSerializer.V1_RESTORED_TOPIC);
            output.writeInt(1);
            output.flush();
            serialized = bytes.toByteArray();
        }

        assertThatThrownBy(() -> INSTANCE.deserialize(2, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("requires zero published messages, but found 1");
    }

    @Test
    void v1OriginMarkerStillRequiresV2MessageCount() throws IOException {
        byte[] serialized =
                INSTANCE.serialize(
                        new PulsarCommittable(new TxnID(1L, 2L), Collections.emptyMap(), 1));
        byte[] truncated = Arrays.copyOf(serialized, serialized.length - Integer.BYTES);

        assertThatThrownBy(() -> INSTANCE.deserialize(2, truncated))
                .isInstanceOf(IOException.class);
    }

    @Test
    void truncatedV1StateFailsDeserialization() {
        assertThatThrownBy(() -> INSTANCE.deserialize(1, new byte[16]))
                .isInstanceOf(IOException.class);
    }

    @Test
    void emptyTopicInV1StateFailsDeserialization() throws IOException {
        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(1L);
            output.writeLong(2L);
            output.writeUTF("");
            output.flush();
            serialized = bytes.toByteArray();
        }

        assertThatThrownBy(() -> INSTANCE.deserialize(1, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("topic must not be empty");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "topic"})
    void unexpectedPlaceholderTopicInV2StateFailsDeserialization(String topic) throws IOException {
        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(1L);
            output.writeLong(2L);
            output.writeUTF(topic);
            output.writeInt(0);
            output.flush();
            serialized = bytes.toByteArray();
        }

        assertThatThrownBy(() -> INSTANCE.deserialize(2, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("but found '" + topic + "'");
    }

    @Test
    void negativeMessageCountInV2StateFailsDeserialization() throws IOException {
        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(1L);
            output.writeLong(2L);
            output.writeUTF(PulsarCommittableSerializer.V2_LEGACY_TOPIC);
            output.writeInt(-1);
            output.flush();
            serialized = bytes.toByteArray();
        }

        assertThatThrownBy(() -> INSTANCE.deserialize(2, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Negative number of published messages");
    }

    @Test
    void oversizedMessageCountInV2StateFailsWithoutPreallocating() throws IOException {
        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(1L);
            output.writeLong(2L);
            output.writeUTF(PulsarCommittableSerializer.V2_LEGACY_TOPIC);
            output.writeInt(Integer.MAX_VALUE);
            output.flush();
            serialized = bytes.toByteArray();
        }

        assertThatThrownBy(() -> INSTANCE.deserialize(2, serialized))
                .isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void trailingBytesInV2StateFailDeserialization(int originVersion) throws IOException {
        PulsarCommittable committable =
                new PulsarCommittable(new TxnID(1L, 2L), Collections.emptyMap(), originVersion);
        byte[] serialized = INSTANCE.serialize(committable);
        byte[] serializedWithTrailingByte = Arrays.copyOf(serialized, serialized.length + 1);

        assertThatThrownBy(() -> INSTANCE.deserialize(2, serializedWithTrailingByte))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Trailing bytes");
    }

    private static MessageIdPojo messageId() {
        return new MessageIdPojo(1L, 2L, 3, 0, -1);
    }
}
