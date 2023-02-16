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

import org.apache.flink.connector.pulsar.common.config.PulsarConfiguration;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.RateLimiter;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RATES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RETRIES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_WAIT_MILLIS;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

/** A wrapper which wraps the {@link PulsarAdmin} with request retry and rate limit support. */
public class PulsarAdminInvocationHandler implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarAdminInvocationHandler.class);

    @SuppressWarnings("java:S3077")
    private static volatile RateLimiter rateLimiter;

    private final PulsarAdmin admin;
    private final int retryTimes;
    private final long waitMillis;
    private final int requestRates;
    private final Map<String, Object> handlers;

    public PulsarAdminInvocationHandler(PulsarAdmin admin, PulsarConfiguration configuration) {
        this.admin = admin;
        this.retryTimes = configuration.get(PULSAR_ADMIN_REQUEST_RETRIES);
        this.waitMillis = configuration.get(PULSAR_ADMIN_REQUEST_WAIT_MILLIS);
        this.requestRates = configuration.get(PULSAR_ADMIN_REQUEST_RATES);
        this.handlers = new ConcurrentHashMap<>();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> returnType = method.getReturnType();

        // No need to proxy the void return type.
        // The non-interface type is not able to proxy.
        if (returnType.equals(Void.TYPE) || !returnType.isInterface()) {
            return method.invoke(admin, args);
        }

        String methodName = method.getName();
        if (handlers.containsKey(methodName)) {
            return handlers.get(methodName);
        }

        Object handler =
                Proxy.newProxyInstance(
                        Thread.currentThread().getContextClassLoader(),
                        new Class[] {returnType},
                        new RequestHandler(method.invoke(admin, args)));
        this.handlers.put(methodName, handler);

        return handler;
    }

    /** A proxy handler with retry support for all the admin request. */
    @SuppressWarnings({"java:S1193", "java:S1181", "java:S3776", "java:S112"})
    private class RequestHandler implements InvocationHandler {

        private final Object handler;

        public RequestHandler(Object handler) {
            this.handler = handler;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return doInvoke(method, args, retryTimes);
        }

        private Object doInvoke(Method method, Object[] args, int remainingTimes) throws Throwable {
            while (true) {
                // Make sure the request is allowed in the given rates.
                requestRateLimit(requestRates);

                try {
                    return method.invoke(handler, args);
                } catch (InvocationTargetException e) {
                    Throwable throwable = e.getTargetException();
                    if (throwable instanceof NotFoundException) {
                        // No need to retry on such exceptions.
                        throw throwable;
                    } else if (throwable instanceof PulsarAdminException) {
                        remainingTimes--;
                        LOG.warn("Request error in Admin API, remain times: {}", remainingTimes, e);
                        if (remainingTimes == 0) {
                            throw throwable;
                        } else {
                            // Sleep for the given times before executing the next query.
                            sleepUninterruptibly(waitMillis, MILLISECONDS);
                        }
                    } else {
                        throw throwable;
                    }
                }
            }
        }
    }

    /** Request a global ratelimit which limits the total admin API request rates. */
    private static void requestRateLimit(int requestRates) {
        if (rateLimiter == null) {
            synchronized (PulsarAdminInvocationHandler.class) {
                if (rateLimiter == null) {
                    rateLimiter = RateLimiter.create(requestRates);
                }
            }
        }

        rateLimiter.acquire();
    }
}
