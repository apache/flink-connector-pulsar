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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A serializer used to serialize {@link PulsarCommittable}. */
public class PulsarCommittableSerializer implements SimpleVersionedSerializer<PulsarCommittable> {

    private static final int VERSION_V1 = 1;
    private static final int VERSION_V2 = 2;

    @Override
    public int getVersion() {
        return VERSION_V2;
    }

    @Override
    public byte[] serialize(PulsarCommittable obj) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(obj.getVersion());
            TxnID txnID = obj.getTxnID();
            output.writeLong(txnID.getMostSigBits());
            output.writeLong(txnID.getLeastSigBits());

            Map<String, MessageIdPojo> messages = obj.getLatestPublishedMessages();
            output.writeInt(messages.size());
            for (Map.Entry<String, MessageIdPojo> entry : messages.entrySet()) {
                MessageIdPojo messageId = entry.getValue();
                output.writeUTF(entry.getKey());
                output.writeLong(messageId.getLedgerId());
                output.writeLong(messageId.getEntryId());
                output.writeInt(messageId.getBatchSize());
                output.writeInt(messageId.getBatchIndex());
                output.writeInt(messageId.getPartitionIndex());
            }
            output.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public PulsarCommittable deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_V1:
                return deserializeV1(serialized);
            case VERSION_V2:
                return deserializeV2(serialized);
            default:
                throw new IOException("Unsupported Pulsar committable version: " + version);
        }
    }

    private PulsarCommittable deserializeV1(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            // V1 state also stores a topic. A transaction can now span topics, so the field is
            // not restored into the committable. Still read and validate it to consume the
            // complete V1 payload and reject corrupted legacy state instead of silently
            // accepting it.
            String topic = in.readUTF();
            if (topic.isEmpty()) {
                throw new IOException("Corrupt V1 committable: topic must not be empty.");
            }
            TxnID txnID = new TxnID(mostSigBits, leastSigBits);
            return new PulsarCommittable(txnID, Collections.emptyMap(), VERSION_V1);
        }
    }

    private PulsarCommittable deserializeV2(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
                DataInputStream input = new DataInputStream(bytes)) {
            int committableVersion = input.readInt();
            if (committableVersion != VERSION_V1 && committableVersion != VERSION_V2) {
                throw new IOException(
                        "Unsupported Pulsar committable state version: " + committableVersion);
            }

            TxnID txnID = new TxnID(input.readLong(), input.readLong());
            int messageCount = input.readInt();
            if (messageCount < 0) {
                throw new IOException("Negative number of published messages: " + messageCount);
            }

            Map<String, MessageIdPojo> messages = new HashMap<>(messageCount);
            for (int i = 0; i < messageCount; i++) {
                String topic = input.readUTF();
                MessageIdPojo messageId =
                        new MessageIdPojo(
                                input.readLong(),
                                input.readLong(),
                                input.readInt(),
                                input.readInt(),
                                input.readInt());
                messages.put(topic, messageId);
            }
            if (input.available() != 0) {
                throw new IOException("Trailing bytes in Pulsar committable state");
            }
            return new PulsarCommittable(txnID, messages, committableVersion);
        }
    }
}
