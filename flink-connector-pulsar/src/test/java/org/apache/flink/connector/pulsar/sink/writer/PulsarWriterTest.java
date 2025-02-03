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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.delayer.FixedMessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.delayer.MessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.router.RoundRobinTopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSchemaWrapper;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.MetadataListener;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_SCHEMA_EVOLUTION;
import static org.apache.flink.metrics.groups.UnregisteredMetricsGroup.createSinkWriterMetricGroup;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PulsarWriter}. */
class PulsarWriterTest extends PulsarTestSuiteBase {

    private static final SinkWriter.Context CONTEXT = new MockSinkWriterContext();

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void writeMessagesToPulsar(DeliveryGuarantee guarantee) throws Exception {
        String topic = "writer-" + randomAlphabetic(10);
        operator().createTopic(topic, 8);
        MetadataListener listener = new MetadataListener(singletonList(topic));

        writeMessageAndVerify(guarantee, listener, topic);
    }

    @Test
    void writeMessagesToPulsarWithTopicAutoCreation() throws Exception {
        String topic = "non-existed-topic-" + randomAlphabetic(10);
        MetadataListener listener = new MetadataListener();

        writeMessageAndVerify(DeliveryGuarantee.AT_LEAST_ONCE, listener, topic);
    }

    private void writeMessageAndVerify(
            DeliveryGuarantee guarantee, MetadataListener listener, String topic) throws Exception {
        SinkConfiguration configuration = sinkConfiguration(guarantee);
        TopicRouter<String> router = new DynamicTopicRouter<>(configuration, topic);
        PulsarSerializationSchema<String> schema = new PulsarSchemaWrapper<>(STRING);
        FixedMessageDelayer<String> delayer = MessageDelayer.never();
        MockInitContext initContext = new MockInitContext();

        PulsarWriter<String> writer =
                new PulsarWriter<>(
                        configuration,
                        schema,
                        listener,
                        router,
                        delayer,
                        PulsarCrypto.disabled(),
                        initContext);

        writer.flush(false);
        writer.prepareCommit();
        writer.flush(false);
        writer.prepareCommit();

        String message = randomAlphabetic(10);
        writer.write(message, CONTEXT);
        writer.flush(false);

        Collection<PulsarCommittable> committables = writer.prepareCommit();
        if (guarantee != EXACTLY_ONCE) {
            assertThat(committables).isEmpty();
        } else {
            assertThat(committables).hasSize(1);
            PulsarCommittable committable =
                    committables.stream().findFirst().orElseThrow(IllegalArgumentException::new);
            TransactionCoordinatorClient coordinatorClient = operator().coordinatorClient();
            coordinatorClient.commit(committable.getTxnID());
        }

        String consumedMessage = operator().receiveMessage(topic, STRING).getValue();
        assertThat(consumedMessage).isEqualTo(message);
    }

    private SinkConfiguration sinkConfiguration(DeliveryGuarantee deliveryGuarantee) {
        Configuration configuration = operator().sinkConfig(deliveryGuarantee);
        configuration.set(PULSAR_WRITE_SCHEMA_EVOLUTION, true);

        return new SinkConfiguration(configuration);
    }

    private static class DynamicTopicRouter<IN> implements TopicRouter<IN> {
        private static final long serialVersionUID = -2855742316603280628L;

        private final RoundRobinTopicRouter<IN> robinTopicRouter;
        private final String topic;

        public DynamicTopicRouter(SinkConfiguration configuration, String topic) {
            this.robinTopicRouter = new RoundRobinTopicRouter<>(configuration);
            this.topic = topic;
        }

        @Override
        public TopicPartition route(
                IN in, String key, List<TopicPartition> partitions, PulsarSinkContext context) {
            if (partitions.isEmpty()) {
                return new TopicPartition(topic);
            } else {
                return robinTopicRouter.route(in, key, partitions, context);
            }
        }
    }

    private static class MockInitContext implements InitContext {

        private final SinkWriterMetricGroup metricGroup;
        private final ProcessingTimeService timeService;

        private MockInitContext() {
            this.metricGroup = createSinkWriterMetricGroup();
            this.timeService = new TestProcessingTimeService();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new SyncMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        // The following three methods are for compatibility with
        // https://github.com/apache/flink/commit/4f5b2fb5736f5a1c098a7dc1d448a879f36f801b
        // . Removed the commented out `@Override` when we move to 1.18.

        // @Override
        public boolean isObjectReuseEnabled() {
            return false;
        }

        // @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        // @Override
        public JobID getJobId() {
            return null;
        }

        @Override
        public JobInfo getJobInfo() {
            return null;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return null;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return new SerializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return metricGroup;
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return null;
                }
            };
        }
    }

    private static class MockSinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }
}
