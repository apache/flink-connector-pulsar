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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarSourceBuilder}. */
class PulsarSourceBuilderTest {

    @Test
    void someSetterMethodCouldOnlyBeCalledOnce() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);

        assertThatThrownBy(() -> builder.setServiceUrl("service-url2"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> builder.setSubscriptionName("set_subscription_name2"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topicPatternAndListCouldChooseOnlyOne() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);
        assertThatThrownBy(() -> builder.setTopicPattern("a-a-a"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void adminUrlIsOptional() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);

        assertThatCode(builder::build).doesNotThrowAnyException();

        builder.setAdminUrl("admin-url");
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void sourceLifecycleDoesNotRequireAdminUrl() throws Exception {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);
        PulsarSource<String> source = builder.build();

        try (SourceReader<String, PulsarPartitionSplit> reader =
                source.createReader(new TestingReaderContext())) {
            reader.start();
            assertThat(reader.snapshotState(1L)).isEmpty();
            reader.notifyCheckpointComplete(1L);
        }

        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                new MockSplitEnumeratorContext<>(1)) {
            PulsarSourceEnumState checkpoint;
            try (SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> enumerator =
                    source.createEnumerator(context)) {
                enumerator.start();
                checkpoint = enumerator.snapshotState(1L);
            }
            try (SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> restored =
                    source.restoreEnumerator(context, checkpoint)) {
                restored.start();
                assertThat(restored.snapshotState(2L))
                        .usingRecursiveComparison()
                        .isEqualTo(checkpoint);
            }
        }
    }

    private void fillRequiredFields(PulsarSourceBuilder<String> builder) {
        builder.setServiceUrl("pulsar://127.0.0.1:6650");
        builder.setSubscriptionName("subscription-name");
        builder.setTopics("topic");
        builder.setDeserializationSchema(Schema.STRING);
    }
}
