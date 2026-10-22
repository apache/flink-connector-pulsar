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

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.api.transaction.TxnID;

import java.util.Map;
import java.util.Objects;

/** The writer state for Pulsar connector. We would used in Pulsar committer. */
@Internal
public class PulsarCommittable {

    /** The transaction id. */
    private final TxnID txnID;

    private final Map<String, MessageIdPojo> latestPublishedMessages;

    private final int version;

    public PulsarCommittable(TxnID txnID, Map<String, MessageIdPojo> latestPublishedMessages) {
        this(txnID, latestPublishedMessages, 2);
    }

    PulsarCommittable(
            TxnID txnID, Map<String, MessageIdPojo> latestPublishedMessages, int version) {
        this.txnID = txnID;
        this.latestPublishedMessages = latestPublishedMessages;
        this.version = version;
    }

    public TxnID getTxnID() {
        return txnID;
    }

    public Map<String, MessageIdPojo> getLatestPublishedMessages() {
        return latestPublishedMessages;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PulsarCommittable)) {
            return false;
        }
        PulsarCommittable that = (PulsarCommittable) o;
        return Objects.equals(txnID, that.txnID)
                && Objects.equals(latestPublishedMessages, that.latestPublishedMessages)
                && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnID, latestPublishedMessages, version);
    }

    @Override
    public String toString() {
        return "PulsarCommittable{"
                + "txnID="
                + txnID
                + ", latestPublishedMessages="
                + latestPublishedMessages
                + ", version="
                + version
                + '}';
    }
}
