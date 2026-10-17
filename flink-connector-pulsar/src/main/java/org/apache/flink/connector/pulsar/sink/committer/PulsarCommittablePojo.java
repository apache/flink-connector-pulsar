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

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;
import java.util.Objects;

/** A POJO representation of {@link PulsarCommittable} used for serialization. */
@PublicEvolving
public class PulsarCommittablePojo {

    private TxnIdPojo txnID;
    private Map<String, MessageIdPojo> latestPublishedMessages;

    public PulsarCommittablePojo() {}

    public PulsarCommittablePojo(
            TxnIdPojo txnID, Map<String, MessageIdPojo> latestPublishedMessages) {
        this.txnID = txnID;
        this.latestPublishedMessages = latestPublishedMessages;
    }

    public TxnIdPojo getTxnID() {
        return txnID;
    }

    public void setTxnID(TxnIdPojo txnID) {
        this.txnID = txnID;
    }

    public Map<String, MessageIdPojo> getLatestPublishedMessages() {
        return latestPublishedMessages;
    }

    public void setLatestPublishedMessages(Map<String, MessageIdPojo> latestPublishedMessages) {
        this.latestPublishedMessages = latestPublishedMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PulsarCommittablePojo)) {
            return false;
        }
        PulsarCommittablePojo that = (PulsarCommittablePojo) o;
        return Objects.equals(txnID, that.txnID)
                && Objects.equals(latestPublishedMessages, that.latestPublishedMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnID, latestPublishedMessages);
    }
}
