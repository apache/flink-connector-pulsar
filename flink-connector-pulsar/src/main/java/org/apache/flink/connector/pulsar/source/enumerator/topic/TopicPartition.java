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

package org.apache.flink.connector.pulsar.source.enumerator.topic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Topic partition is the basic topic information used by {@link SplitReader} and {@link
 * TopicRouter}.
 */
@PublicEvolving
public class TopicPartition implements Serializable {
    private static final long serialVersionUID = -1474354741550810953L;

    /**
     * If {@link TopicPartition#getPartitionId()} is equal to this. This topic partition wouldn't be
     * a partition instance. It would be a non-partitioned topic name.
     */
    public static final int NON_PARTITION_ID = -1;

    private static final List<TopicRange> FULL_RANGES = ImmutableList.of(createFullRange());

    /**
     * The topic name of the pulsar. It would be a full topic name. If you don't provide the tenant
     * and namespace, we would add them automatically.
     */
    private final String topic;

    /**
     * Index of partition for the topic. It would be a natural number for the partitioned topic with
     * a non-key_shared subscription.
     */
    private final int partitionId;

    /**
     * The ranges for this topic, used for limiting consume scope. It would be a {@link
     * TopicRange#createFullRange()} full range for all the subscription type except {@link
     * SubscriptionType#Key_Shared}.
     */
    private final List<TopicRange> ranges;

    /** Create a non-partition topic without partition information. */
    @PublicEvolving
    public TopicPartition(String topic) {
        this(topic, NON_PARTITION_ID, FULL_RANGES);
    }

    /** Create a topic partition without key hash range. */
    @PublicEvolving
    public TopicPartition(String topic, int partitionId) {
        this(topic, partitionId, FULL_RANGES);
    }

    /** Create a non-partition topic with key hash range. */
    @Internal
    public TopicPartition(String topic, List<TopicRange> ranges) {
        this(topic, NON_PARTITION_ID, ranges);
    }

    @Internal
    public TopicPartition(String topic, int partitionId, List<TopicRange> ranges) {
        checkArgument(partitionId >= NON_PARTITION_ID, "Invalid partition id.");

        this.topic = topicName(checkNotNull(topic));
        this.partitionId = partitionId;
        this.ranges = checkNotNull(ranges);
    }

    public String getTopic() {
        return topic;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isPartition() {
        return partitionId != NON_PARTITION_ID;
    }

    /**
     * Pulsar split the topic partition into a bunch of small topics, we would get the real topic
     * name by using this method.
     */
    public String getFullTopicName() {
        if (isPartition()) {
            return topicNameWithPartition(topic, partitionId);
        } else {
            return topic;
        }
    }

    /** This method is internal used for serialization. */
    @Internal
    public List<TopicRange> getRanges() {
        return ranges;
    }

    /** This method is internal used for define key shared subscription. */
    @Internal
    public List<Range> getPulsarRanges() {
        return ranges.stream().map(TopicRange::toPulsarRange).collect(toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicPartition partition = (TopicPartition) o;

        return partitionId == partition.partitionId
                && topic.equals(partition.topic)
                && ranges.equals(partition.ranges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partitionId, ranges);
    }

    @Override
    public String toString() {
        return getFullTopicName() + "|" + ranges;
    }
}
