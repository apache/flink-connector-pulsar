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

package org.apache.flink.connector.pulsar.testutils.runtime.container;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;
import static org.testcontainers.containers.wait.strategy.Wait.forHttp;

/**
 * {@link PulsarRuntime} implementation, use the TestContainers as the backend. We would start a
 * pulsar container by this provider.
 */
public class PulsarContainerRuntime implements PulsarRuntime {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarContainerRuntime.class);

    // The default host for connecting in docker environment.
    private static final String PULSAR_INTERNAL_HOSTNAME = "pulsar";
    // This url is used on the container side.
    private static final String PULSAR_SERVICE_URL =
            String.format("pulsar://%s:%d", PULSAR_INTERNAL_HOSTNAME, BROKER_PORT);
    // This url is used on the container side.
    private static final String PULSAR_ADMIN_URL =
            String.format("http://%s:%d", PULSAR_INTERNAL_HOSTNAME, BROKER_HTTP_PORT);

    private static final DockerImageName PULSAR_IMAGE_NAME =
            DockerImageName.parse("apachepulsar/pulsar");
    private static final String CURRENT_VERSION = "2.10.2";
    private static final DockerImageName PULSAR_IMAGE = PULSAR_IMAGE_NAME.withTag(CURRENT_VERSION);

    private final PulsarContainer container;
    private final AtomicBoolean started;
    private final Map<String, String> brokerConfigs;

    private boolean boundFlink = false;
    private PulsarRuntimeOperator operator;

    public PulsarContainerRuntime() {
        this.container = new PulsarContainer(PULSAR_IMAGE);
        this.started = new AtomicBoolean(false);
        this.brokerConfigs = new HashMap<>();

        brokerConfigs.put("transactionCoordinatorEnabled", "true");
        brokerConfigs.put("acknowledgmentAtBatchIndexLevelEnabled", "true");
        brokerConfigs.put("systemTopicEnabled", "true");
        brokerConfigs.put("defaultNumberOfNamespaceBundles", "1");
        brokerConfigs.put("allowAutoTopicCreation", "true");
        brokerConfigs.put("allowAutoTopicCreationType", "partitioned");
        brokerConfigs.put("defaultNumPartitions", "4");
    }

    public PulsarContainerRuntime bindWithFlinkContainer(GenericContainer<?> flinkContainer) {
        checkArgument(
                !started.get(),
                "This Pulsar container has been started, we can't bind it to a Flink container.");

        this.container
                .withNetworkAliases(PULSAR_INTERNAL_HOSTNAME)
                .dependsOn(flinkContainer)
                .withNetwork(flinkContainer.getNetwork());
        this.boundFlink = true;
        return this;
    }

    @Override
    public PulsarRuntime setConfigs(Map<String, String> configs) {
        brokerConfigs.putAll(configs);
        return this;
    }

    @Override
    public void startUp() {
        if (!started.compareAndSet(false, true)) {
            LOG.warn("You have started the Pulsar Container. We will skip this execution.");
            return;
        }

        // Override the default standalone configuration by system environments.
        brokerConfigs.forEach((key, value) -> container.withEnv("PULSAR_PREFIX_" + key, value));
        // Change the default bootstrap script, it will override the default configuration
        // and start a standalone Pulsar without streaming storage and function worker.
        container.withCommand(
                "sh",
                "-c",
                "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf && /pulsar/bin/pulsar standalone --no-functions-worker -nss");
        // Waiting for the Pulsar broker and the transaction is ready after the container started.
        container.waitingFor(
                forHttp(
                                "/admin/v2/persistent/pulsar/system/transaction_coordinator_assign/partitions")
                        .forPort(BROKER_HTTP_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(5)));

        // Start the Pulsar Container.
        container.start();
        // Append the output to this runtime logger. Used for local debug purpose.
        container.followOutput(new Slf4jLogConsumer(LOG, true));

        // Create the operator.
        if (boundFlink) {
            this.operator =
                    new PulsarRuntimeOperator(
                            container.getPulsarBrokerUrl(),
                            PULSAR_SERVICE_URL,
                            container.getHttpServiceUrl(),
                            PULSAR_ADMIN_URL);
        } else {
            this.operator =
                    new PulsarRuntimeOperator(
                            container.getPulsarBrokerUrl(), container.getHttpServiceUrl());
        }
    }

    @Override
    public void tearDown() {
        try {
            if (operator != null) {
                operator.close();
            }
            container.stop();
            started.compareAndSet(true, false);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public PulsarRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this pulsar container first.");
    }
}
