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

package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTO_CERT_REFRESH_TIME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECT_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_READ_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_REQUEST_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests direct construction of the Pulsar admin client. */
class PulsarClientFactoryTest {

    @Test
    void createsAdminWithAdminConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_ADMIN_URL, "http://localhost:8080");
        configuration.set(PULSAR_CONNECT_TIMEOUT, 1);
        configuration.set(PULSAR_READ_TIMEOUT, 2);
        configuration.set(PULSAR_REQUEST_TIMEOUT, 3);
        configuration.set(PULSAR_AUTO_CERT_REFRESH_TIME, 4);

        assertThatCode(
                        () -> {
                            PulsarAdmin admin =
                                    PulsarClientFactory.createAdmin(
                                            new SinkConfiguration(configuration));
                            admin.close();
                        })
                .doesNotThrowAnyException();
    }
}
