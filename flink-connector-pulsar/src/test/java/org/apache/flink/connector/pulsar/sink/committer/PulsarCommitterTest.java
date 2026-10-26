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

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_ABORTED;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_UNCOMMITTED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the transaction state checks used to make commit recovery idempotent. */
class PulsarCommitterTest {

    private static final String TOPIC = "persistent://public/default/topic";
    private static final TxnID TXN_ID = new TxnID(1L, 2L);
    private static final MessageIdPojo MESSAGE_ID = new MessageIdPojo(10L, 20L, 1, 0, -1);

    private PulsarAdmin pulsarAdmin;
    private Topics topics;
    private TransactionCoordinatorClient coordinatorClient;

    @BeforeEach
    void setUp() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        coordinatorClient = mock(TransactionCoordinatorClient.class);
        when(pulsarAdmin.topics()).thenReturn(topics);
    }

    @Test
    void commitsUncommittedTransaction() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> request = request();
        Message<byte[]> message = messageWithProperty(TXN_UNCOMMITTED, "true");
        when(topics.getMessagesById(TOPIC, 10L, 20L))
                .thenReturn(Collections.singletonList(message));

        committer.commit(Collections.singletonList(request));

        verify(coordinatorClient).commit(TXN_ID);
        verify(request, never()).signalAlreadyCommitted();
    }

    @Test
    void marksCommittedTransactionAsAlreadyCommitted() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> request = request();
        Message<byte[]> message = messageWithProperty("other", "value");
        when(topics.getMessagesById(TOPIC, 10L, 20L))
                .thenReturn(Collections.singletonList(message));

        committer.commit(Collections.singletonList(request));

        verify(request).signalAlreadyCommitted();
        verify(coordinatorClient, never()).commit(any());
    }

    @Test
    void failsAbortedTransaction() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> request = request();
        Message<byte[]> message = messageWithProperty(TXN_ABORTED, "true");
        when(topics.getMessagesById(TOPIC, 10L, 20L))
                .thenReturn(Collections.singletonList(message));

        committer.commit(Collections.singletonList(request));

        verify(request).signalFailedWithKnownReason(any());
        verify(coordinatorClient, never()).commit(any());
    }

    @Test
    void retriesWhenAdminCannotQueryMessage() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> request = request();
        when(topics.getMessagesById(TOPIC, 10L, 20L))
                .thenThrow(new PulsarAdminException("unavailable"));

        committer.commit(Collections.singletonList(request));

        verify(request).retryLater();
    }

    @Test
    void commitsMultipleLegacyCommittablesWithoutAdminLookups() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> first = request(legacyCommittable(TXN_ID));
        CommitRequest<PulsarCommittable> second = request(legacyCommittable(new TxnID(3L, 4L)));
        doNothing().when(coordinatorClient).commit(TXN_ID);
        doNothing().when(coordinatorClient).commit(new TxnID(3L, 4L));

        committer.commit(Arrays.asList(first, second));

        verify(coordinatorClient).commit(TXN_ID);
        verify(coordinatorClient).commit(new TxnID(3L, 4L));
        verify(topics, never()).getMessagesById(any(), anyLong(), anyLong());
    }

    @Test
    void checksEachCurrentCommittableWhenRecoveringMultipleCheckpoints() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> first = request();
        TxnID secondTxnId = new TxnID(3L, 4L);
        MessageIdPojo secondMessageId = new MessageIdPojo(11L, 21L, 1, 0, -1);
        CommitRequest<PulsarCommittable> second =
                request(
                        new PulsarCommittable(
                                secondTxnId,
                                Collections.singletonMap(TOPIC, secondMessageId),
                                PulsarCommittableSerializer.CURRENT_VERSION));
        Message<byte[]> firstMessage = messageWithProperty("other", "value");
        Message<byte[]> secondMessage = messageWithProperty("other", "value");
        when(topics.getMessagesById(TOPIC, 10L, 20L))
                .thenReturn(Collections.singletonList(firstMessage));
        when(topics.getMessagesById(TOPIC, 11L, 21L))
                .thenReturn(Collections.singletonList(secondMessage));

        committer.commit(Arrays.asList(first, second));

        verify(first).signalAlreadyCommitted();
        verify(second).signalAlreadyCommitted();
        verify(coordinatorClient, never()).commit(any());
    }

    @Test
    void commitsSingleLegacyCommittableWithoutAdminLookups() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> request = request(legacyCommittable(TXN_ID));
        doNothing().when(coordinatorClient).commit(TXN_ID);

        committer.commit(Collections.singletonList(request));

        verify(coordinatorClient).commit(TXN_ID);
        verify(topics, never()).getMessagesById(any(), anyLong(), anyLong());
    }

    @Test
    void emptyTransactionDoesNotFailRecovery() throws Exception {
        PulsarCommitter committer = committer();
        CommitRequest<PulsarCommittable> request =
                request(
                        new PulsarCommittable(
                                TXN_ID,
                                Collections.emptyMap(),
                                PulsarCommittableSerializer.CURRENT_VERSION));
        doThrow(new RuntimeException("already finalized")).when(coordinatorClient).commit(TXN_ID);

        committer.commit(Collections.singletonList(request));

        verify(request).signalAlreadyCommitted();
    }

    @Test
    void closeClosesBothClientAndAdmin() throws Exception {
        PulsarCommitter committer = committer();
        PulsarClient pulsarClient = mock(PulsarClient.class);
        setField(committer, "pulsarClient", pulsarClient);

        committer.close();

        verify(pulsarClient).close();
        verify(pulsarAdmin).close();
    }

    @Test
    void closeClosesAdminWhenClientCloseFails() throws Exception {
        PulsarCommitter committer = committer();
        PulsarClient pulsarClient = mock(PulsarClient.class);
        setField(committer, "pulsarClient", pulsarClient);
        doThrow(new PulsarClientException("client close failed")).when(pulsarClient).close();

        assertThatThrownBy(committer::close).isInstanceOf(PulsarClientException.class);

        verify(pulsarAdmin).close();
    }

    private PulsarCommitter committer() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_ADMIN_URL, "http://localhost:8080");
        configuration.set(PULSAR_SERVICE_URL, "pulsar://localhost:6650");
        PulsarCommitter committer =
                new PulsarCommitter(
                        new SinkConfiguration(configuration), mock(CommitterInitContext.class));
        setField(committer, "pulsarAdmin", pulsarAdmin);
        setField(committer, "coordinatorClient", coordinatorClient);
        return committer;
    }

    private CommitRequest<PulsarCommittable> request() {
        return request(TXN_ID);
    }

    private CommitRequest<PulsarCommittable> request(TxnID txnId) {
        return request(
                new PulsarCommittable(
                        txnId,
                        Collections.singletonMap(TOPIC, MESSAGE_ID),
                        PulsarCommittableSerializer.CURRENT_VERSION));
    }

    private PulsarCommittable legacyCommittable(TxnID txnId) {
        return new PulsarCommittable(txnId, Collections.emptyMap(), 1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CommitRequest<PulsarCommittable> request(PulsarCommittable committable) {
        CommitRequest<PulsarCommittable> request = mock(CommitRequest.class);
        when(request.getCommittable()).thenReturn(committable);
        when(request.getNumberOfRetries()).thenReturn(0);
        return request;
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> messageWithProperty(String key, String value) {
        Message<byte[]> message = mock(Message.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(key, value);
        when(message.getProperties()).thenReturn(properties);
        return message;
    }

    private static void setField(PulsarCommitter committer, String fieldName, Object value)
            throws ReflectiveOperationException {
        Field field = PulsarCommitter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(committer, value);
    }
}
