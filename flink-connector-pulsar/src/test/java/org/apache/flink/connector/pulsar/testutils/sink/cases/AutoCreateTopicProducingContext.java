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

package org.apache.flink.connector.pulsar.testutils.sink.cases;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.sink.PulsarSinkTestContext;

/**
 * Sink the messages into a non-existed topic and test the connector could auto create it. This test
 * case only gets passed when the {@code allowAutoTopicCreation} is enabled on the Pulsar runtime.
 */
public class AutoCreateTopicProducingContext extends PulsarSinkTestContext {

    public AutoCreateTopicProducingContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected boolean creatTopic() {
        return false;
    }

    @Override
    protected String displayName() {
        return "write messages into a non-existed topic in Pulsar";
    }
}
