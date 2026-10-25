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

/**
 * A serializer used to serialize {@link PulsarCommittable}.
 *
 * <p>Each version only appends fields to the previous one, so connector versions that only know V1
 * can still read V2 state.
 */
public class PulsarCommittableSerializer implements SimpleVersionedSerializer<PulsarCommittable> {

    private static final int CURRENT_VERSION = 2;
    private static final String V2_LEGACY_TOPIC = "unknown_due_to_v2_serialization";

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PulsarCommittable obj) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            TxnID txnID = obj.getTxnID();
            output.writeLong(txnID.getMostSigBits());
            output.writeLong(txnID.getLeastSigBits());
            // V1 topic field. A transaction can now span topics, so use a descriptive placeholder.
            output.writeUTF(V2_LEGACY_TOPIC);

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
        if (version < 1 || version > CURRENT_VERSION) {
            throw new IOException("Unsupported Pulsar committable version: " + version);
        }
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            TxnID txnID = new TxnID(mostSigBits, leastSigBits);
            // V1 topic field, unused since a transaction can span topics.
            String topic = in.readUTF();
            if (topic.isEmpty()) {
                throw new IOException("Corrupt V1 committable: topic must not be empty.");
            }

            Map<String, MessageIdPojo> messages = Collections.emptyMap();
            if (version >= CURRENT_VERSION) {
                int messageCount = in.readInt();
                if (messageCount < 0) {
                    throw new IOException("Negative number of published messages: " + messageCount);
                }

                messages = new HashMap<>();
                for (int i = 0; i < messageCount; i++) {
                    String messageTopic = in.readUTF();
                    MessageIdPojo messageId =
                            new MessageIdPojo(
                                    in.readLong(),
                                    in.readLong(),
                                    in.readInt(),
                                    in.readInt(),
                                    in.readInt());
                    messages.put(messageTopic, messageId);
                }
            }
            if (in.available() != 0) {
                throw new IOException("Trailing bytes in Pulsar committable state");
            }
            return new PulsarCommittable(txnID, messages);
        }
    }
}
