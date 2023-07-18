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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.resourcePath;

/** Shared utilities for building Flink containers. */
public class FlinkContainerUtils {

    public static Configuration flinkConfiguration() {
        Configuration configuration = new Configuration();
        TaskExecutorResourceUtils.adjustForLocalExecution(configuration);

        // Increase the jvm metaspace memory to avoid java.lang.OutOfMemoryError: Metaspace
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(2048));
        configuration.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(512));
        configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(2560));
        configuration.set(JobManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(1024));

        return configuration;
    }

    public static List<URL> connectorJarPaths() {
        List<URL> urls = new ArrayList<>();
        urls.add(resourcePath("pulsar-connector.jar"));
        urls.add(resourcePath("flink-connector-testing.jar"));
        return urls;
    }
}
