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

import java.util.Objects;

/** A POJO representation of Pulsar message ID used for serialization. */
@PublicEvolving
public class MessageIdPojo {

    private int batchIndex;
    private int batchSize;
    private long ledgerId;
    private long entryId;
    private int partitionIndex;

    public MessageIdPojo() {}

    public MessageIdPojo(
            long ledgerId, long entryId, int batchSize, int batchIndex, int partitionIndex) {
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.partitionIndex = partitionIndex;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public void setBatchIndex(int batchIndex) {
        this.batchIndex = batchIndex;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public void setPartitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MessageIdPojo)) {
            return false;
        }
        MessageIdPojo that = (MessageIdPojo) o;
        return batchIndex == that.batchIndex
                && batchSize == that.batchSize
                && ledgerId == that.ledgerId
                && entryId == that.entryId
                && partitionIndex == that.partitionIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchIndex, batchSize, ledgerId, entryId, partitionIndex);
    }
}
