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

package org.apache.flink.connector.pulsar.sink.writer.message;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.writer.router.KeyHashTopicRouter;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link TypedMessageBuilder} wrapper for providing the required method for end-users. */
@PublicEvolving
public class PulsarMessageBuilder<T> {

    private byte[] orderingKey;
    private String key;
    private long eventTime;
    @Nullable private final Schema<T> schema;
    @Nullable private final T value;
    private final Map<String, String> properties = new HashMap<>();
    private Long sequenceId;
    private List<String> replicationClusters;
    private boolean disableReplication = false;

    /**
     * Make this constructor package private for in favor of the {@link PulsarMessage#builder()}
     * method in {@link PulsarMessage}.
     */
    PulsarMessageBuilder(@Nullable Schema<T> schema, @Nullable T value) {
        this.schema = schema;
        this.value = value;
    }

    /** Method wrapper of {@link TypedMessageBuilder#orderingKey(byte[])}. */
    public PulsarMessageBuilder<T> orderingKey(byte[] orderingKey) {
        this.orderingKey = checkNotNull(orderingKey);
        return this;
    }

    /**
     * Method wrapper of {@link TypedMessageBuilder#key(String)}. This key would also be used in
     * {@link KeyHashTopicRouter}.
     */
    public PulsarMessageBuilder<T> key(String key) {
        this.key = checkNotNull(key);
        return this;
    }

    /**
     * Method wrapper of {@link TypedMessageBuilder#eventTime(long)}. If you don't provide the event
     * time, we will try to use Flink's sink context time instead.
     */
    public PulsarMessageBuilder<T> eventTime(long eventTime) {
        checkArgument(eventTime > 0, "The given event time should above 0.");
        this.eventTime = eventTime;
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#property(String, String)}. */
    public PulsarMessageBuilder<T> property(String key, String value) {
        this.properties.put(checkNotNull(key), checkNotNull(value));
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#properties(Map)}. */
    public PulsarMessageBuilder<T> properties(Map<String, String> properties) {
        this.properties.putAll(checkNotNull(properties));
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#sequenceId(long)}. */
    public PulsarMessageBuilder<T> sequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#replicationClusters(List)}. */
    public PulsarMessageBuilder<T> replicationClusters(List<String> replicationClusters) {
        this.replicationClusters = checkNotNull(replicationClusters);
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#disableReplication()}. */
    public PulsarMessageBuilder<T> disableReplication() {
        this.disableReplication = true;
        return this;
    }

    public PulsarMessage<T> build() {
        return new PulsarMessage<>(
                orderingKey,
                key,
                eventTime,
                schema,
                value,
                properties,
                sequenceId,
                replicationClusters,
                disableReplication);
    }
}
