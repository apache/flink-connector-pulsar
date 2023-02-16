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

package org.apache.flink.connector.pulsar.common.handler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;

import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.Properties;
import org.apache.pulsar.client.admin.ProxyStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.ResourceGroups;
import org.apache.pulsar.client.admin.ResourceQuotas;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Sink;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.admin.Source;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.admin.Worker;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RATES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RETRIES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_WAIT_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test of {@link PulsarAdminInvocationHandler}. */
class PulsarAdminInvocationHandlerTest {

    @Test
    void retryWithFixedTimesOnRequestFailure() {
        int retryTimes = 5 + ThreadLocalRandom.current().nextInt(5);
        String errorMsg = "Always failed with reason: " + randomAlphanumeric(10);

        PulsarAdminTestImpl admin = new PulsarAdminTestImpl(errorMsg);

        Configuration configuration = new Configuration();
        configuration.set(PULSAR_ADMIN_REQUEST_RETRIES, retryTimes);
        configuration.set(PULSAR_ADMIN_REQUEST_WAIT_MILLIS, 50L);
        configuration.set(PULSAR_ADMIN_REQUEST_RATES, 1000);
        SinkConfiguration sinkConfiguration = new SinkConfiguration(configuration);

        PulsarAdminInvocationHandler handler =
                new PulsarAdminInvocationHandler(admin, sinkConfiguration);

        PulsarAdmin proxyAdmin =
                (PulsarAdmin)
                        Proxy.newProxyInstance(
                                PulsarAdmin.class.getClassLoader(),
                                new Class[] {PulsarAdmin.class},
                                handler);

        assertThatThrownBy(() -> proxyAdmin.lookups().lookupPartitionedTopic("aa"))
                .isInstanceOf(PulsarAdminException.class)
                .hasMessage(errorMsg);
        assertThat(admin.lookup.callingTimes()).isEqualTo(retryTimes);
    }

    @Test
    void didNotRetryOnNotFoundException() {
        PulsarAdminTestImpl admin = new PulsarAdminTestImpl("not found");

        PulsarAdminInvocationHandler handler =
                new PulsarAdminInvocationHandler(admin, new SinkConfiguration(new Configuration()));

        PulsarAdmin proxyAdmin =
                (PulsarAdmin)
                        Proxy.newProxyInstance(
                                PulsarAdmin.class.getClassLoader(),
                                new Class[] {PulsarAdmin.class},
                                handler);

        assertThatThrownBy(() -> proxyAdmin.lookups().lookupTopic("some"))
                .isInstanceOf(NotFoundException.class)
                .hasMessage("not found");
        assertThat(admin.lookup.callingTimes()).isEqualTo(1);
    }

    /** Test implementation for PulsarAdmin. */
    private static final class PulsarAdminTestImpl implements PulsarAdmin {

        private final LookupTestImpl lookup;

        private PulsarAdminTestImpl(String errorMsg) {
            this.lookup = new LookupTestImpl(errorMsg);
        }

        @Override
        public Clusters clusters() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Brokers brokers() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Tenants tenants() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public ResourceGroups resourcegroups() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Properties properties() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Namespaces namespaces() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Topics topics() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public TopicPolicies topicPolicies() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public TopicPolicies topicPolicies(boolean isGlobal) {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Bookies bookies() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public NonPersistentTopics nonPersistentTopics() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public ResourceQuotas resourceQuotas() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Lookup lookups() {
            lookup.resetTimes();
            return lookup;
        }

        @Override
        public Functions functions() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Source source() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Sources sources() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Sink sink() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Sinks sinks() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Worker worker() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public BrokerStats brokerStats() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public ProxyStats proxyStats() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public String getServiceUrl() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Schemas schemas() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Packages packages() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Transactions transactions() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }
    }

    /** Test implementation for Lookup. */
    private static final class LookupTestImpl implements Lookup {

        private final String errorMsg;
        private int times;

        private LookupTestImpl(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        @Override
        public String lookupTopic(String topic) throws PulsarAdminException {
            times++;
            throw new NotFoundException(
                    new IllegalArgumentException("not found"), "not found", 404);
        }

        @Override
        public CompletableFuture<String> lookupTopicAsync(String topic) {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public Map<String, String> lookupPartitionedTopic(String topic)
                throws PulsarAdminException {
            times++;
            throw new PulsarAdminException(errorMsg);
        }

        @Override
        public CompletableFuture<Map<String, String>> lookupPartitionedTopicAsync(String topic) {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public String getBundleRange(String topic) {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        @Override
        public CompletableFuture<String> getBundleRangeAsync(String topic) {
            throw new UnsupportedOperationException("We didn't support this method in test.");
        }

        public int callingTimes() {
            return times;
        }

        public void resetTimes() {
            this.times = 0;
        }
    }
}
