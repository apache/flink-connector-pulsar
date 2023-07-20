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

import org.apache.flink.connector.pulsar.common.MiniClusterTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContextFactory;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.source.cases.EncryptedMessagesConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.MultipleTopicsConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.PartialKeysConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.SingleTopicConsumingContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.apache.pulsar.client.api.SubscriptionType;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

/**
 * Unit test class for {@link PulsarSource}. Used for {@link SubscriptionType#Exclusive}
 * subscription.
 */
class PulsarSourceITCase extends SourceTestSuiteBase<String> {

    // Defines test environment on Flink MiniCluster
    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines pulsar running environment
    @TestExternalSystem
    PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.container());

    // This field is preserved, we don't support the semantics in source currently.
    @TestSemantics CheckpointingMode[] semantics = new CheckpointingMode[] {EXACTLY_ONCE};

    // Defines an external context Factories,
    // so test cases will be invoked using these external contexts.
    @TestContext
    PulsarTestContextFactory<String, SingleTopicConsumingContext> singleTopic =
            new PulsarTestContextFactory<>(pulsar, SingleTopicConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, MultipleTopicsConsumingContext> multipleTopic =
            new PulsarTestContextFactory<>(pulsar, MultipleTopicsConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, PartialKeysConsumingContext> partialKeys =
            new PulsarTestContextFactory<>(pulsar, PartialKeysConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, EncryptedMessagesConsumingContext> encryptMessages =
            new PulsarTestContextFactory<>(pulsar, EncryptedMessagesConsumingContext::new);
}
