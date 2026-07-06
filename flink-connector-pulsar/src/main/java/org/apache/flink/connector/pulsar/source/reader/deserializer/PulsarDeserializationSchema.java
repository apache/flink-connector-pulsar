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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.io.Serializable;

/**
 * A schema bridge for deserializing the pulsar's {@code Message<byte[]>} into a flink managed
 * instance. We support both the pulsar's {@link Schema} and flink's {@link DeserializationSchema}.
 *
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
@SuppressWarnings("java:S112")
public interface PulsarDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link PulsarInitializationContext} can be used to access additional features
     * such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     * @param configuration The Pulsar related source configuration.
     */
    default void open(PulsarInitializationContext context, SourceConfiguration configuration)
            throws Exception {}

    /**
     * Deserializes the pulsar message. This message could be a raw byte message or some parsed
     * message which is decoded by pulsar schema.
     *
     * <p>You can output multiple messages by using the {@link Collector}. Note that number and size
     * of the produced records should be relatively small. Depending on the source's implementation
     * records can be buffered in memory or collecting records might delay the emitting checkpoint
     * barrier.
     *
     * @param message The message decoded by pulsar.
     * @param out The collector to put the resulting messages.
     */
    void deserialize(Message<byte[]> message, Collector<T> out) throws Exception;

    /** An interface for providing extra schema initial context for users. */
    @PublicEvolving
    public interface PulsarInitializationContext extends InitializationContext {

        /** Return the internal client for extra dynamic features. */
        PulsarClient getPulsarClient();
    }
}
