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

import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Properties;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_DELIVERY_GUARANTEE;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.CUSTOM;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.MESSAGE_KEY_HASH;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.ROUND_ROBIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void serviceUrlIsRequiredButAdminUrlIsOptionalByDefault() {
        PulsarSinkBuilder<String> builder = PulsarSink.builder();
        builder.setSerializationSchema(new SimpleStringSchema());
        builder.setTopics("a", "b");
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);

        builder.setServiceUrl("pulsar://127.0.0.1:8888");
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = {"NONE", "AT_LEAST_ONCE"})
    void adminUrlIsOptionalWithoutExactlyOnce(DeliveryGuarantee guarantee) throws Exception {
        PulsarSinkBuilder<String> builder = configuredBuilder().setDeliveryGuarantee(guarantee);

        try (Committer<PulsarCommittable> committer = builder.build().createCommitter(null)) {
            // Flink creates a committer even when the writer produces no committables.
            committer.commit(Collections.emptyList());
        }
        builder.setAdminUrl("http://127.0.0.1:8080");
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = {"NONE", "AT_LEAST_ONCE"})
    void writerLifecycleDoesNotRequireAdminUrl(DeliveryGuarantee guarantee) throws Exception {
        PulsarSink<String> sink =
                PulsarSink.<String>builder()
                        .setServiceUrl("pulsar://127.0.0.1:6650")
                        // An explicit partition needs no broker lookup during initialization.
                        .setTopics("topic-partition-0")
                        .setSerializationSchema(new SimpleStringSchema())
                        .setDeliveryGuarantee(guarantee)
                        .build();
        WriterInitContext context = mock(WriterInitContext.class);
        when(context.getTaskInfo()).thenReturn(new TaskInfoImpl("test", 1, 0, 1, 0));
        when(context.getProcessingTimeService()).thenReturn(mock(ProcessingTimeService.class));
        when(context.metricGroup()).thenReturn(mock(SinkWriterMetricGroup.class));

        try (CommittingSinkWriter<String, PulsarCommittable> writer = sink.createWriter(context)) {
            writer.flush(false);
            assertThat(writer.prepareCommit()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t\n"})
    void exactlyOnceRejectsBlankAdminUrl(String adminUrl) {
        PulsarSinkBuilder<String> builder =
                configuredBuilder()
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setAdminUrl(adminUrl);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PULSAR_ADMIN_URL.key())
                .hasMessageContaining("non-blank")
                .hasMessageContaining("EXACTLY_ONCE");
    }

    @Test
    void exactlyOnceRequiresAdminUrl() {
        PulsarSinkBuilder<String> builder =
                configuredBuilder().setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pulsar.admin.adminUrl")
                .hasMessageContaining("EXACTLY_ONCE");

        builder.setAdminUrl("http://127.0.0.1:8080");
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void exactlyOnceConfiguredThroughPropertiesRequiresAdminUrl() {
        Properties properties = new Properties();
        properties.setProperty(
                PULSAR_WRITE_DELIVERY_GUARANTEE.key(), DeliveryGuarantee.EXACTLY_ONCE.toString());
        PulsarSinkBuilder<String> builder = configuredBuilder().setProperties(properties);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pulsar.admin.adminUrl");

        properties = new Properties();
        properties.setProperty("pulsar.admin.adminUrl", "http://127.0.0.1:8080");
        builder.setProperties(properties);
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void exactlyOnceConfiguredThroughConfigurationRequiresAdminUrl() {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_WRITE_DELIVERY_GUARANTEE, DeliveryGuarantee.EXACTLY_ONCE);
        PulsarSinkBuilder<String> builder = configuredBuilder().setConfig(configuration);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PULSAR_ADMIN_URL.key());

        configuration.set(PULSAR_ADMIN_URL, "https://127.0.0.1:8443");
        builder.setConfig(configuration);
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    private PulsarSinkBuilder<String> configuredBuilder() {
        return PulsarSink.<String>builder()
                .setServiceUrl("pulsar://127.0.0.1:8888")
                .setTopics("topic")
                .setSerializationSchema(new SimpleStringSchema());
    }
}
