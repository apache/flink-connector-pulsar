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

package org.apache.flink.connector.pulsar.testutils.runtime.remote;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;

/** The runtime which will connect to a remote instance. It's always used for local debugging. */
public class PulsarRemoteRuntime implements PulsarRuntime {

    private final String serviceUrl;
    private final String adminUrl;

    private PulsarRuntimeOperator operator;

    public PulsarRemoteRuntime(String host) {
        this("pulsar://" + host + ":" + BROKER_PORT, "http://" + host + ":" + BROKER_HTTP_PORT);
    }

    public PulsarRemoteRuntime(String serviceUrl, String adminUrl) {
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
    }

    @Override
    public PulsarRuntime withConfigs(Map<String, String> configs) {
        if (!configs.isEmpty()) {
            throw new UnsupportedOperationException(
                    "We can't change the broker configs on a running instance.");
        }

        return this;
    }

    @Override
    public void startUp() throws Exception {
        this.operator = new PulsarRuntimeOperator(serviceUrl, adminUrl);
    }

    @Override
    public void tearDown() {
        // Nothing to do here.
    }

    @Override
    public PulsarRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this pulsar runtime first.");
    }
}
