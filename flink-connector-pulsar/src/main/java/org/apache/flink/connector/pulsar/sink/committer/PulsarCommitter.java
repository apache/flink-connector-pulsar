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
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorNotFoundException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.getTcClient;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_ABORTED;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_UNCOMMITTED;
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
    private PulsarAdmin pulsarAdmin;
    private TransactionCoordinatorClient coordinatorClient;

    public PulsarCommitter(
            SinkConfiguration sinkConfiguration, CommitterInitContext committerInitContext) {
        this.sinkConfiguration = checkNotNull(sinkConfiguration);
    }

    @Override
    @SuppressWarnings("java:S3776")
    public void commit(Collection<CommitRequest<PulsarCommittable>> requests)
            throws PulsarClientException {
        if (requests.isEmpty()) {
            return;
        }

        for (CommitRequest<PulsarCommittable> request : requests) {
            // Checkpoint state written by older connector versions does not contain message IDs,
            // so preserve its direct-commit behavior. Current state carries message IDs and must
            // always use them for recovery checks, including when several checkpoints are
            // recovered together.
            if (request.getCommittable().getVersion() == 1) {
                commitLegacyTransaction(request);
            } else {
                commitCurrentTransaction(request);
            }
        }
    }

    private void commitCurrentTransaction(CommitRequest<PulsarCommittable> request)
            throws PulsarClientException {
        PulsarCommittable committable = request.getCommittable();
        TxnID txnID = committable.getTxnID();
        Map<String, MessageIdPojo> latestMessages = committable.getLatestPublishedMessages();
        TransactionCoordinatorClient client = transactionCoordinatorClient();

        // A transaction without output has no observable message state. Its outcome is irrelevant
        // to the result, so a failure here must not make checkpoint recovery fail.
        if (latestMessages.isEmpty()) {
            try {
                client.commit(txnID);
            } catch (Exception e) {
                LOG.warn("Failed to commit empty transaction {}.", txnID, e);
                request.signalAlreadyCommitted();
            }
            return;
        }

        int failedQueries = 0;
        for (Map.Entry<String, MessageIdPojo> entry : latestMessages.entrySet()) {
            String topic = entry.getKey();
            MessageIdPojo messageId = entry.getValue();
            TopicName topicName = TopicName.get(topic);
            String messageTopic =
                    messageId.getPartitionIndex() == -1 || topicName.isPartitioned()
                            ? topic
                            : topicName.getPartition(messageId.getPartitionIndex()).toString();

            final List<Message<byte[]>> messages;
            try {
                messages =
                        pulsarAdmin
                                .topics()
                                .getMessagesById(
                                        messageTopic,
                                        messageId.getLedgerId(),
                                        messageId.getEntryId());
            } catch (PulsarAdminException e) {
                failedQueries++;
                LOG.warn("Failed to query message {} on topic {}.", messageId, messageTopic, e);
                continue;
            }

            if (messages == null || messages.isEmpty()) {
                continue;
            }

            Map<String, String> properties = messages.get(0).getProperties();
            if ("true".equalsIgnoreCase(properties.get(TXN_ABORTED))) {
                String message =
                        String.format(
                                "Transaction %s was aborted; messages: %s", txnID, latestMessages);
                LOG.warn(message);
                request.signalFailedWithKnownReason(new FlinkRuntimeException(message));
                return;
            }

            if ("true".equalsIgnoreCase(properties.get(TXN_UNCOMMITTED))) {
                try {
                    client.commit(txnID);
                } catch (Exception e) {
                    handleError(request, txnID, committable, e);
                }
                return;
            }

            request.signalAlreadyCommitted();
            return;
        }

        if (failedQueries > 0) {
            int maxRecommitTimes = sinkConfiguration.getMaxRecommitTimes();
            if (request.getNumberOfRetries() < maxRecommitTimes) {
                request.retryLater();
            } else {
                String message =
                        String.format(
                                "Unable to query messages for transaction %s after %d retries: %s",
                                txnID, maxRecommitTimes, latestMessages);
                request.signalFailedWithKnownReason(new FlinkRuntimeException(message));
            }
            return;
        }

        String message =
                String.format(
                        "All messages for transaction %s have expired: %s", txnID, latestMessages);
        LOG.warn(message);
        request.signalFailedWithKnownReason(new FlinkRuntimeException(message));
    }

    private void commitLegacyTransaction(CommitRequest<PulsarCommittable> request)
            throws PulsarClientException {
        TransactionCoordinatorClient client = transactionCoordinatorClient();
        PulsarCommittable committable = request.getCommittable();
        TxnID txnID = committable.getTxnID();
        LOG.debug("Start committing the Pulsar transaction {}", txnID);
        try {
            client.commit(txnID);
        } catch (Exception e) {
            handleError(request, txnID, committable, e);
        }
    }

    private void handleError(
            CommitRequest<PulsarCommittable> request,
            TxnID txnID,
            PulsarCommittable committable,
            Exception e) {
        if (e instanceof CoordinatorNotFoundException) {
            LOG.error(
                    "We couldn't find the Transaction Coordinator from Pulsar broker {}. "
                            + "Check your broker configuration.",
                    committable,
                    e);
            request.signalFailedWithKnownReason(e);
        } else if (e instanceof InvalidTxnStatusException) {
            LOG.error(
                    "Unable to commit transaction {} because it is in an invalid state.",
                    committable,
                    e);
            request.signalAlreadyCommitted();
        } else if (e instanceof TransactionNotFoundException) {
            if (request.getNumberOfRetries() == 0) {
                request.signalFailedWithKnownReason(e);
            } else {
                LOG.warn(
                        "Transaction {} was not found after retrying; treating it as committed.",
                        txnID,
                        e);
                request.signalAlreadyCommitted();
            }
        } else if (e instanceof MetaStoreHandlerNotExistsException) {
            LOG.error(
                    "We can't find the meta store handler for transaction {} in {}.",
                    committable,
                    TRANSACTION_COORDINATOR_ASSIGN,
                    e);
            request.signalFailedWithKnownReason(e);
        } else if (e instanceof TransactionCoordinatorClientException) {
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
        } else {
            LOG.error("Transaction {} encountered an unknown commit error.", committable, e);
            request.signalFailedWithUnknownReason(e);
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
            PulsarClient client = createClient(sinkConfiguration);
            PulsarAdmin admin = null;
            try {
                admin = createAdmin(sinkConfiguration);
                TransactionCoordinatorClient coordinator = getTcClient(client);

                this.pulsarClient = client;
                this.pulsarAdmin = admin;
                this.coordinatorClient = coordinator;
            } catch (PulsarClientException | RuntimeException e) {
                if (admin != null) {
                    try {
                        admin.close();
                    } catch (RuntimeException closeException) {
                        e.addSuppressed(closeException);
                    }
                }
                try {
                    client.close();
                } catch (Exception closeException) {
                    e.addSuppressed(closeException);
                }
                throw e;
            }
        }

        return coordinatorClient;
    }

    @Override
    public void close() throws IOException {
        Exception closeException = null;
        if (pulsarClient != null) {
            try {
                pulsarClient.close();
            } catch (Exception e) {
                closeException = e;
            }
        }
        if (pulsarAdmin != null) {
            try {
                pulsarAdmin.close();
            } catch (Exception e) {
                if (closeException == null) {
                    closeException = e;
                } else {
                    closeException.addSuppressed(e);
                }
            }
        }
        if (closeException != null) {
            if (closeException instanceof IOException) {
                throw (IOException) closeException;
            }
            if (closeException instanceof RuntimeException) {
                throw (RuntimeException) closeException;
            }
            throw new IOException("Failed to close Pulsar committer resources.", closeException);
        }
    }
}
