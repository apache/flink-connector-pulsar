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

package org.apache.flink.connector.pulsar.testutils.source.cases;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.source.PulsarSourceTestContext;
import org.apache.flink.util.FlinkRuntimeException;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;

/**
 * Pulsar external context that will create multiple topics with only one partitions as source
 * splits.
 */
public class MultipleTopicsConsumingContext extends PulsarSourceTestContext {

    private final String topicPrefix =
            "public/default/flink-multiple-topic-" + randomAlphabetic(8) + "-";

    private int index = 0;

    public MultipleTopicsConsumingContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected String displayName() {
        return "consume message on multiple topic";
    }

    @Override
    protected String topicPattern() {
        return topicPrefix + "\\d+";
    }

    @Override
    protected String subscriptionName() {
        return "flink-multiple-topic-test";
    }

    @Override
    protected String generatePartitionName() {
        String topic = topicPrefix + index;
        try {
            operator.createTopic(topic, 1);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        index++;

        return topicNameWithPartition(topic, 0);
    }
}
