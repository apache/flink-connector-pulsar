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

import org.apache.flink.connector.pulsar.common.handler.PulsarAdminInvocationHandler;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector;
import org.apache.pulsar.client.api.PulsarClientException;

import java.lang.reflect.Proxy;

/**
 * {@link org.apache.pulsar.client.admin.PulsarAdminBuilder} didn't expose all the configurations to
 * end user. We have to extend the default builder method for adding extra configurations.
 */
public class PulsarAdminProxyBuilder extends PulsarAdminBuilderImpl {

    private final PulsarConfiguration configuration;

    public PulsarAdminProxyBuilder(PulsarConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * This is used by the internal implementation of {@link AsyncHttpConnector} to set the max
     * allowed number of request threads.
     */
    public void numIoThreads(int numIoThreads) {
        conf.setNumIoThreads(numIoThreads);
    }

    /**
     * Wrap the pulsar admin interface into a proxy instance which can retry the request and limit
     * the request rate.
     */
    @Override
    public PulsarAdmin build() throws PulsarClientException {
        PulsarAdminInvocationHandler handler =
                new PulsarAdminInvocationHandler(super.build(), configuration);
        return (PulsarAdmin)
                Proxy.newProxyInstance(
                        PulsarAdmin.class.getClassLoader(),
                        new Class[] {PulsarAdmin.class},
                        handler);
    }
}
