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

package org.apache.flink.connector.pulsar.source.enumerator.cursor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumerator;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ChunkMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The class for defining the start or stop position. We only expose the constructor for end user.
 */
@PublicEvolving
public final class CursorPosition implements Serializable {
    private static final long serialVersionUID = -802405183307684549L;

    private final Type type;

    private final MessageId messageId;
    private final boolean include;

    private final Long timestamp;

    /**
     * Start consuming from the given message id. The message id couldn't be the {@code
     * MultipleMessageIdImpl}.
     *
     * @param include Whether the cosponsored position will be (in/ex)cluded in the consuming
     *     result.
     */
    public CursorPosition(MessageId messageId, boolean include) {
        checkNotNull(messageId, "Message id couldn't be null.");

        this.type = Type.MESSAGE_ID;
        this.messageId = messageId;
        this.include = include;
        this.timestamp = null;
    }

    /**
     * Start consuming from the given timestamp position. The cosponsored position will be included
     * in the consuming result.
     */
    public CursorPosition(Long timestamp) {
        checkNotNull(timestamp, "Timestamp couldn't be null.");

        this.type = Type.TIMESTAMP;
        this.messageId = null;
        this.include = true;
        this.timestamp = timestamp;
    }

    /** This method is used to create the initial position in {@link PulsarSourceEnumerator}. */
    @Internal
    public void setupSubPosition(PulsarClient client, String topicName, String subscriptionName)
            throws PulsarClientException {
        try (Consumer<GenericRecord> consumer =
                client.newConsumer(new AutoConsumeSchema())
                        .topic(topicName)
                        .subscriptionName(subscriptionName)
                        .subscribe()) {
            // Reset cursor to desired position.
            if (type == Type.TIMESTAMP) {
                consumer.seek(getActualTimestamp(this.timestamp));
            } else if (messageId instanceof ChunkMessageIdImpl) {
                MessageIdAdv msgId = ((ChunkMessageIdImpl) messageId).getFirstChunkMessageId();
                consumer.seek(getActualMessageId(msgId));
            } else {
                consumer.seek(getActualMessageId((MessageIdAdv) messageId));
            }
        }
    }

    private MessageId getActualMessageId(MessageIdAdv messageIdImpl) {
        if (include) {
            return messageIdImpl;
        } else {
            // if the message is batched, should return next single message in current batch.
            if (messageIdImpl.getBatchIndex() >= 0 && messageIdImpl.getBatchSize() > 0
                    && messageIdImpl.getBatchIndex() != messageIdImpl.getBatchSize() - 1) {
                return new BatchMessageIdImpl(messageIdImpl.getLedgerId(),
                        messageIdImpl.getEntryId(),
                        messageIdImpl.getPartitionIndex(),
                        messageIdImpl.getBatchIndex() + 1,
                        messageIdImpl.getBatchSize(),
                        messageIdImpl.getAckSet());
            }

            // if the (ledgerId, entryId + 1) is not valid
            // pulsar broker will automatically set the cursor to the next valid message
            return new MessageIdImpl(
                    messageIdImpl.getLedgerId(),
                    messageIdImpl.getEntryId() + 1,
                    messageIdImpl.getPartitionIndex());
        }
    }

    private long getActualTimestamp(long timestamp) {
        if (include) {
            return timestamp;
        } else {
            return timestamp + 1;
        }
    }

    @Override
    public String toString() {
        if (type == Type.TIMESTAMP) {
            return "timestamp: " + timestamp;
        } else {
            return "message id: " + messageId + " include: " + include;
        }
    }

    /**
     * The position type for reader to choose whether timestamp or message id as the start position.
     */
    @Internal
    public enum Type {
        TIMESTAMP,

        MESSAGE_ID
    }
}
