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

package org.apache.flink.connector.pulsar.testutils.runtime;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.container.PulsarContainerRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.remote.PulsarRemoteRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.singleton.PulsarSingletonRuntime;

import org.testcontainers.containers.GenericContainer;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * An abstraction for different pulsar runtimes. Providing the common methods for {@link
 * PulsarTestEnvironment}.
 *
 * <p>All the Pulsar runtime should enable the transaction by default.
 */
public interface PulsarRuntime {

    /**
     * Override the default broker configs with some new values. This method is called before
     * executing the {@link #startUp()} method.
     */
    PulsarRuntime withConfigs(Map<String, String> configs);

    default PulsarRuntime withConfig(String key, String value) {
        return withConfigs(singletonMap(key, value));
    }

    /** Start up this pulsar runtime, block the thread until everytime is ready for this runtime. */
    void startUp() throws Exception;

    /** Shutdown this pulsar runtime. */
    void tearDown() throws Exception;

    /**
     * Return an operator for operating this pulsar runtime. This operator predefined a set of
     * extremely useful methods for Pulsar. You can easily add new methods in this operator.
     */
    PulsarRuntimeOperator operator();

    /** Connect to a remote running instance. */
    static PulsarRuntime remote(String host) {
        return new PulsarRemoteRuntime(host);
    }

    /** Connect to a remote running instance. */
    static PulsarRuntime remote(String serviceUrl, String adminUrl) {
        return new PulsarRemoteRuntime(serviceUrl, adminUrl);
    }

    /**
     * A singleton instance of the Pulsar docker instance in the whole test lifecycle. We will start
     * the instance only once and didn't stop it after the tests finished. The instance will be
     * shared among all the tests.
     */
    static PulsarRuntime singletonContainer() {
        return PulsarSingletonRuntime.INSTANCE;
    }

    /**
     * Create a Pulsar instance in docker. We would start a standalone Pulsar in TestContainers.
     * This runtime is often used in end-to-end tests. The stream storage for bookkeeper is
     * disabled. The function worker is disabled on Pulsar broker.
     */
    static PulsarRuntime container() {
        return new PulsarContainerRuntime();
    }

    /**
     * Create a Pulsar instance in docker. We would start a standalone Pulsar in TestContainers.
     * This runtime is often used in end-to-end tests. The stream storage for bookkeeper is
     * disabled. The function worker is disabled on Pulsar broker.
     *
     * <p>We would link the created Pulsar docker instance with the given flink instance. This would
     * enable the connection for Pulsar and Flink in a docker environment.
     */
    static PulsarRuntime container(GenericContainer<?> flinkContainer) {
        return new PulsarContainerRuntime().bindWithFlinkContainer(flinkContainer);
    }
}
