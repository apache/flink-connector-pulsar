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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/** A serializer used to serialize {@link PulsarCommittable}. */
public class PulsarCommittableSerializer implements SimpleVersionedSerializer<PulsarCommittable> {

    private static final int VERSION_V1 = 1;
    private static final int VERSION_V2 = 2;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, false);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false);
    }

    @Override
    public int getVersion() {
        return VERSION_V2;
    }

    @Override
    public byte[] serialize(PulsarCommittable obj) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(obj);
    }

    @Override
    public PulsarCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version == VERSION_V1) {
            return deserializeV1(serialized);
        }
        return deserializeV2(serialized);
    }

    private PulsarCommittable deserializeV1(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            TxnID txnID = new TxnID(mostSigBits, leastSigBits);
            return new PulsarCommittable(txnID);
        }
    }

    private PulsarCommittable deserializeV2(byte[] serialized) throws IOException {
        PulsarCommittablePojo pulsarCommittablePojo =
                OBJECT_MAPPER.readValue(serialized, PulsarCommittablePojo.class);
        TxnID txnID =
                new TxnID(
                        pulsarCommittablePojo.getTxnID().getMostSigBits(),
                        pulsarCommittablePojo.getTxnID().getLeastSigBits());
        return new PulsarCommittable(txnID, pulsarCommittablePojo.getLatestPublishedMessages());
    }
}
