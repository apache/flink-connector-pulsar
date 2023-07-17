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

package org.apache.flink.tests.util.pulsar.common;

import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContextFactory;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;

import java.util.function.Function;

/**
 * This class is used for creating the Pulsar test context which will running in the Flink
 * containers.
 */
public class PulsarContainerTestContextFactory<F, T extends PulsarTestContext<F>>
        extends PulsarTestContextFactory<F, T> {

    public PulsarContainerTestContextFactory(
            PulsarTestEnvironment environment, Function<PulsarTestEnvironment, T> contextFactory) {
        super(environment, contextFactory);
    }

    @Override
    public T createExternalContext(String testName) {
        T context = super.createExternalContext(testName);
        context.inContainer();
        return context;
    }
}
