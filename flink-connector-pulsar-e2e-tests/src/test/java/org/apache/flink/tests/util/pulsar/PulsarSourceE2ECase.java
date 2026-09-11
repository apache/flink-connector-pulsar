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

package org.apache.flink.tests.util.pulsar;

import org.apache.flink.connector.pulsar.testutils.SimpleCollectIteratorAssert;
import org.apache.flink.connector.pulsar.testutils.source.cases.MultipleTopicsConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.PartialKeysConsumingContext;
import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.tests.util.pulsar.common.FlinkContainerUtils;
import org.apache.flink.tests.util.pulsar.common.PulsarContainerTestEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.execution.CheckpointingMode.EXACTLY_ONCE;

/** Pulsar source E2E test based on the connector testing framework. */
@SuppressWarnings("unused")
public class PulsarSourceE2ECase extends SourceTestSuiteBase<String> {

    // Defines the Semantic.
    @TestSemantics CheckpointingMode[] semantics = new CheckpointingMode[] {EXACTLY_ONCE};

    // Defines TestEnvironment.
    @TestEnv
    FlinkContainerTestEnvironment flink =
            FlinkContainerTestEnvironment.fromSettings(
                    FlinkContainersSettings.builder()
                            .basedOn(FlinkContainerUtils.flinkConfiguration())
                            .numTaskManagers(1)
                            .numSlotsPerTaskManager(6)
                            .build());

    // Defines ConnectorExternalSystem.
    @TestExternalSystem
    PulsarContainerTestEnvironment pulsar = new PulsarContainerTestEnvironment(flink);

    // Defines a set of external context Factories for different test cases.
    @TestContext
    ExternalContextFactory<MultipleTopicsConsumingContext> multipleTopic =
            ignore -> {
                final MultipleTopicsConsumingContext context =
                        new MultipleTopicsConsumingContext(pulsar);
                context.addConnectorJarPaths(FlinkContainerUtils.connectorJarPaths());
                return context;
            };

    @TestContext
    ExternalContextFactory<PartialKeysConsumingContext> partialKeys =
            ignore -> {
                final PartialKeysConsumingContext context = new PartialKeysConsumingContext(pulsar);
                context.addConnectorJarPaths(FlinkContainerUtils.connectorJarPaths());
                return context;
            };

    /**
     * {@link CollectIteratorAssertions} will generate a mismatch description if the result does not
     * match the expected value. It attempts to capture all following messages, even though already
     * failed, which helps engineers for troubleshooting, but draining the following messages may
     * lead the test to get stuck. We rewrite the method to avoid the test to get stuck.
     */
    @Override
    protected void checkResultWithSemantic(
            CloseableIterator<String> resultIterator,
            List<List<String>> testData,
            org.apache.flink.core.execution.CheckpointingMode semantic,
            Integer limit) {
        if (limit != null) {
            Runnable runnable =
                    () ->
                            new SimpleCollectIteratorAssert<>(resultIterator)
                                    .withNumRecordsLimit(limit)
                                    .matchesRecordsFromSource(testData, semantic);

            try {
                CompletableFuture.runAsync(runnable).get(65, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError("Timed out or failed while validating source results.", e);
            }
        } else {
            new SimpleCollectIteratorAssert<>(resultIterator)
                    .matchesRecordsFromSource(testData, semantic);
        }
    }
}
