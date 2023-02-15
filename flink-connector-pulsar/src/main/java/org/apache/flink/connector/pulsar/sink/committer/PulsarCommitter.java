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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorNotFoundException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.getTcClient;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.common.naming.SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN;

/**
 * Committer implementation for {@link PulsarSink}.
 *
 * <p>The committer is responsible to finalize the Pulsar transactions by committing them.
 */
@Internal
public class PulsarCommitter implements Committer<PulsarCommittable>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCommitter.class);

    private final SinkConfiguration sinkConfiguration;

    private PulsarClient pulsarClient;
    private TransactionCoordinatorClient coordinatorClient;

    public PulsarCommitter(SinkConfiguration sinkConfiguration) {
        this.sinkConfiguration = checkNotNull(sinkConfiguration);
    }

    @Override
    @SuppressWarnings("java:S3776")
    public void commit(Collection<CommitRequest<PulsarCommittable>> requests)
            throws PulsarClientException {
        TransactionCoordinatorClient client = transactionCoordinatorClient();

        for (CommitRequest<PulsarCommittable> request : requests) {
            PulsarCommittable committable = request.getCommittable();
            TxnID txnID = committable.getTxnID();
            String topic = committable.getTopic();

            LOG.debug("Start committing the Pulsar transaction {} for topic {}", txnID, topic);
            try {
                client.commit(txnID);
            } catch (CoordinatorNotFoundException e) {
                LOG.error(
                        "We couldn't find the Transaction Coordinator from Pulsar broker {}. "
                                + "Check your broker configuration.",
                        committable,
                        e);
                request.signalFailedWithKnownReason(e);
            } catch (InvalidTxnStatusException e) {
                LOG.error(
                        "Unable to commit transaction ({}) because it's in an invalid state. "
                                + "Most likely the transaction has been aborted for some reason. "
                                + "Please check the Pulsar broker logs for more details.",
                        committable,
                        e);
                request.signalAlreadyCommitted();
            } catch (TransactionNotFoundException e) {
                if (request.getNumberOfRetries() == 0) {
                    LOG.error(
                            "Unable to commit transaction ({}) because it's not found on Pulsar broker. "
                                    + "Most likely the checkpoint interval exceed the transaction timeout.",
                            committable,
                            e);
                    request.signalFailedWithKnownReason(e);
                } else {
                    LOG.warn(
                            "We can't find the transaction {} after {} retry committing. "
                                    + "This may mean that the transaction have been committed in previous but failed with timeout. "
                                    + "So we just mark it as committed.",
                            txnID,
                            request.getNumberOfRetries());
                    request.signalAlreadyCommitted();
                }
            } catch (MetaStoreHandlerNotExistsException e) {
                LOG.error(
                        "We can't find the meta store handler by the mostSigBits from TxnID {}. "
                                + "Did you change the metadata for topic {}?",
                        committable,
                        TRANSACTION_COORDINATOR_ASSIGN,
                        e);
                request.signalFailedWithKnownReason(e);
            } catch (TransactionCoordinatorClientException e) {
                LOG.error(
                        "Encountered retriable exception while committing transaction {} for topic {}.",
                        committable,
                        topic,
                        e);
                int maxRecommitTimes = sinkConfiguration.getMaxRecommitTimes();
                if (request.getNumberOfRetries() < maxRecommitTimes) {
                    request.retryLater();
                } else {
                    String message =
                            String.format(
                                    "Failed to commit transaction %s after retrying %d times",
                                    txnID, maxRecommitTimes);
                    request.signalFailedWithKnownReason(new FlinkRuntimeException(message, e));
                }
            } catch (Exception e) {
                LOG.error(
                        "Transaction ({}) encountered unknown error and data could be potentially lost.",
                        committable,
                        e);
                request.signalFailedWithUnknownReason(e);
            }
        }
    }

    /**
     * Lazy initialize this backend Pulsar client. This committer may not be used in {@link
     * DeliveryGuarantee#NONE} and {@link DeliveryGuarantee#AT_LEAST_ONCE}. So we couldn't create
     * the Pulsar client at first.
     */
    private TransactionCoordinatorClient transactionCoordinatorClient()
            throws PulsarClientException {
        if (coordinatorClient == null) {
            this.pulsarClient = createClient(sinkConfiguration);
            this.coordinatorClient = getTcClient(pulsarClient);
        }

        return coordinatorClient;
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
    }
}
