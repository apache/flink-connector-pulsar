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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.unwrap;

/** A suit of workarounds for the Pulsar Transaction. */
@Internal
public final class PulsarTransactionUtils {

    private PulsarTransactionUtils() {
        // No public constructor
    }

    /** Create transaction with given timeout millis. */
    public static Transaction createTransaction(PulsarClient pulsarClient, long timeoutMs) {
        try {
            CompletableFuture<Transaction> future =
                    sneakyClient(pulsarClient::newTransaction)
                            .withTransactionTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                            .build();

            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new FlinkRuntimeException(unwrap(e));
        }
    }

    /**
     * {@link PulsarClient} didn't expose the internal {@link TransactionCoordinatorClient} to the
     * end user. But the connector needs it to manually commit/abort the transaction by {@link
     * TxnID}.
     */
    public static TransactionCoordinatorClient getTcClient(PulsarClient pulsarClient) {
        TransactionCoordinatorClient coordinatorClient =
                ((PulsarClientImpl) pulsarClient).getTcClient();
        // Ensure you have enabled transaction.
        checkNotNull(coordinatorClient, "You haven't enable transaction in Pulsar client.");

        return coordinatorClient;
    }
}
