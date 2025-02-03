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

package org.apache.flink.connector.pulsar.sink.callback;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;

import org.apache.pulsar.client.api.MessageId;

/**
 * An optional callback interface that users can plug into PulsarSink.
 *
 * @param <IN> The input type of the sink
 */
@PublicEvolving
public interface SinkUserCallback<IN> extends AutoCloseable {
    /**
     * This method is called before the message is sent to the topic. The user can modify the
     * message. By default, the same message will be returned.
     *
     * @param element the element received from the previous operator.
     * @param message the message wrapper with the element already serialized.
     * @param topic the pulsar topic or partition that the message will be routed to.
     */
    void beforeSend(IN element, PulsarMessage<?> message, String topic);

    /**
     * This method is called after producer has tried to write the message to the topic.
     *
     * @param element the element received from the previous operator.
     * @param message the message that was sent to the topic.
     * @param messageId the topic MessageId, if the send operation was successful.
     */
    void onSendSucceeded(IN element, PulsarMessage<?> message, String topic, MessageId messageId);

    /**
     * This method is called after producer has tried to write the message to the topic.
     *
     * @param element the element received from the previous operator.
     * @param message the message that was sent to the topic.
     * @param topic the topic or partition that the message was sent to.
     * @param ex the exception.
     */
    void onSendFailed(IN element, PulsarMessage<?> message, String topic, Throwable ex);
}
