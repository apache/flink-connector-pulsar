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

package org.apache.flink.connector.pulsar.source.callback;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.api.Message;

/**
 * An optional interface that users can plug into PulsarSource.
 *
 * @param <T> The output type of Source
 */
@PublicEvolving
public interface SourceUserCallback<T> extends AutoCloseable {
    /**
     * This method is called after the message is handed off to the Collector, with the raw message
     * from pulsar, as well as the deserialized value.
     *
     * <p>Modifications to the message will not carry forward.
     *
     * @param rawMessage the raw message from the pulsar topic
     * @param deserializedElement the deserialized message body
     */
    void process(Message<byte[]> rawMessage, T deserializedElement);
}
