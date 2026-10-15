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

/** A POJO representation of Pulsar transaction ID used for serialization. */
@PublicEvolving
public class TxnIdPojo {
    private long mostSigBits;
    private long leastSigBits;

    public TxnIdPojo() {}

    public TxnIdPojo(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

    public long getMostSigBits() {
        return mostSigBits;
    }

    public void setMostSigBits(long mostSigBits) {
        this.mostSigBits = mostSigBits;
    }

    public long getLeastSigBits() {
        return leastSigBits;
    }

    public void setLeastSigBits(long leastSigBits) {
        this.leastSigBits = leastSigBits;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TxnIdPojo)) {
            return false;
        }
        TxnIdPojo txnIdPojo = (TxnIdPojo) o;
        return mostSigBits == txnIdPojo.mostSigBits && leastSigBits == txnIdPojo.leastSigBits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mostSigBits, leastSigBits);
    }
}
