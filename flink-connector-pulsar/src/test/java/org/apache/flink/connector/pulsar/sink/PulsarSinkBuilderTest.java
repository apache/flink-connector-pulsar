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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.CUSTOM;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.MESSAGE_KEY_HASH;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.ROUND_ROBIN;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarSinkBuilder}. */
class PulsarSinkBuilderTest {

    @Test
    void topicNameCouldBeSetOnlyOnce() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();
        builder.setTopics("a", "b");

        assertThatThrownBy(() -> builder.setTopics("c")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void topicRoutingModeCouldNotBeCustom() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();

        assertThatCode(() -> builder.setTopicRoutingMode(ROUND_ROBIN)).doesNotThrowAnyException();
        assertThatCode(() -> builder.setTopicRoutingMode(MESSAGE_KEY_HASH))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> builder.setTopicRoutingMode(CUSTOM))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void setConfigCouldNotOverrideExistedConfigs() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();
        builder.setConfig(PULSAR_SEND_TIMEOUT_MS, 1L);

        assertThatCode(() -> builder.setConfig(PULSAR_SEND_TIMEOUT_MS, 1L))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> builder.setConfig(PULSAR_SEND_TIMEOUT_MS, 2L))
                .isInstanceOf(IllegalArgumentException.class);

        Configuration configuration = new Configuration();
        configuration.set(PULSAR_SEND_TIMEOUT_MS, 3L);
        assertThatThrownBy(() -> builder.setConfig(configuration))
                .isInstanceOf(IllegalArgumentException.class);

        Properties properties = new Properties();
        properties.put(PULSAR_SEND_TIMEOUT_MS.key(), 4L);
        assertThatThrownBy(() -> builder.setProperties(properties))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void serializationSchemaIsRequired() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serializationSchema must be set.");
    }

    @Test
    void emptyTopicShouldHaveCustomTopicRouter() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();
        builder.setSerializationSchema(new SimpleStringSchema());

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("No topic names or custom topic router are provided.");
    }

    @Test
    void serviceUrlMustBeProvided() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();
        builder.setSerializationSchema(new SimpleStringSchema());
        builder.setTopics("a", "b");
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);

        builder.setServiceUrl("pulsar+ssl://127.0.0.1:8888");
        assertThatCode(builder::build).doesNotThrowAnyException();
    }
}
