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

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.sink.cases.SingleTopicProducingContext;
import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.tests.util.pulsar.common.FlinkContainerUtils;
import org.apache.flink.tests.util.pulsar.common.PulsarContainerTestEnvironment;

import static org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE;
import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

/** Pulsar sink E2E test based on the connector testing framework. */
@SuppressWarnings("unused")
public class PulsarSinkE2ECase extends SinkTestSuiteBase<String> {

    // Defines the Semantic.
    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {AT_LEAST_ONCE, EXACTLY_ONCE};

    // Defines TestEnvironment
    @TestEnv
    FlinkContainerTestEnvironment flink =
            new FlinkContainerTestEnvironment(FlinkContainerUtils.flinkConfiguration(), 1, 6);

    // Defines ConnectorExternalSystem.
    @TestExternalSystem PulsarTestEnvironment pulsar = new PulsarContainerTestEnvironment(flink);

    // Defines a set of external context Factories for different test cases.
    @TestContext
    ExternalContextFactory<SingleTopicProducingContext> sinkContext =
            ignore -> {
                final SingleTopicProducingContext context = new SingleTopicProducingContext(pulsar);
                context.addConnectorJarPaths(FlinkContainerUtils.connectorJarPaths());
                return context;
            };
}
