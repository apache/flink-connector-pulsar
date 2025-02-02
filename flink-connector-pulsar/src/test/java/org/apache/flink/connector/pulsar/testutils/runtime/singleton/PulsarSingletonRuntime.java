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

package org.apache.flink.connector.pulsar.testutils.runtime.singleton;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.Uninterruptibles;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A singleton instance of the Pulsar docker instance in the whole test lifecycle. We will start the
 * instance only once and didn't stop it after the tests finished. The instance will be shared among
 * all the tests.
 *
 * <p>This runtime can't be used in end-to-end tests.
 */
public enum PulsarSingletonRuntime implements PulsarRuntime {
    INSTANCE {
        private final PulsarContainerRuntime container = new PulsarContainerRuntime();
        private final AtomicReference<RuntimeStatus> status =
                new AtomicReference<>(RuntimeStatus.STOPPED);

        @Override
        public PulsarRuntime withConfigs(Map<String, String> configs) {
            return container.withConfigs(configs);
        }

        @Override
        public void startUp() {
            if (status.compareAndSet(RuntimeStatus.STOPPED, RuntimeStatus.STARTING)) {
                // Entering the start operation.
                try {
                    container.startUp();
                    status.set(RuntimeStatus.STARTED);
                } catch (Exception e) {
                    status.set(RuntimeStatus.FATAL);
                    // Crash the test thread.
                    throw new FlinkRuntimeException(e);
                }
            } else {
                // Other thread may have started the runtime. Waiting for the final status.
                while (status.get() != RuntimeStatus.STARTED
                        && status.get() != RuntimeStatus.FATAL) {
                    Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(500));
                }
                if (status.get() == RuntimeStatus.FATAL) {
                    // Crash the test thread.
                    throw new FlinkRuntimeException(
                            "Fall to start this singleton container. Some error occurs.");
                }
            }
        }

        @Override
        public void tearDown() {
            // Nothing to do here. TestContainers will take care of stopping the Docker instance.
            // See
            // https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/
        }

        @Override
        public PulsarRuntimeOperator operator() {
            return container.operator();
        }
    };

    /** The runtime signal which is used to share the status among different test threads. */
    private enum RuntimeStatus {
        STARTED,
        STARTING,
        STOPPED,
        FATAL,
    }
}
