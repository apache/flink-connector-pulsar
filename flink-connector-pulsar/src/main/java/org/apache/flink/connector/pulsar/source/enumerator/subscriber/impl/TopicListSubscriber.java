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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.common.naming.TopicName;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isPartition;

/** the implements of consuming multiple topics. */
public class TopicListSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 6473918213832993116L;

    private final Set<String> partitions;
    private final Set<String> fullTopicNames;

    public TopicListSubscriber(List<String> fullTopicNameOrPartitions) {
        this.partitions = new HashSet<>();
        this.fullTopicNames = new HashSet<>();

        for (String fullTopicNameOrPartition : fullTopicNameOrPartitions) {
            if (isPartition(fullTopicNameOrPartition)) {
                this.partitions.add(fullTopicNameOrPartition);
            } else {
                this.fullTopicNames.add(fullTopicNameOrPartition);
            }
        }
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            RangeGenerator generator, int parallelism)
            throws ExecutionException, InterruptedException {

        // Query topics from Pulsar.
        Set<TopicPartition> results = createTopicPartitions(fullTopicNames, generator, parallelism);

        // Query partitions from Pulsar.
        for (String partition : partitions) {
            TopicName topicName = TopicName.get(partition);
            String name = topicName.getPartitionedTopicName();
            int index = topicName.getPartitionIndex();
            TopicMetadata metadata = queryTopicMetadata(partition);
            if (metadata != null) {
                List<TopicRange> ranges = generator.range(metadata, parallelism);
                results.add(new TopicPartition(name, index, ranges));
            }
        }

        return results;
    }
}
