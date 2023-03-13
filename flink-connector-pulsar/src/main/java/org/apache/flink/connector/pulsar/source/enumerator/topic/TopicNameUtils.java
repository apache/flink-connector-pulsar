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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.common.naming.TopicName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.naming.SystemTopicNames.isSystemTopic;

/** util for topic name. */
@Internal
public final class TopicNameUtils {

    private static final Pattern HEARTBEAT_NAMESPACE_PATTERN =
            Pattern.compile("pulsar/[^/]+/([^:]+:\\d+)");
    private static final Pattern HEARTBEAT_NAMESPACE_PATTERN_V2 =
            Pattern.compile("pulsar/([^:]+:\\d+)");
    private static final Pattern SLA_NAMESPACE_PATTERN =
            Pattern.compile("sla-monitor" + "/[^/]+/([^:]+:\\d+)");

    private TopicNameUtils() {
        // No public constructor.
    }

    /** Ensure the given topic name should be a topic without partition information. */
    public static String topicName(String topic) {
        return TopicName.get(topic).getPartitionedTopicName();
    }

    /** Create a topic name with partition information. */
    public static String topicNameWithPartition(String topic, int partitionId) {
        checkArgument(partitionId >= 0, "Illegal partition id %s", partitionId);
        return TopicName.get(topic).getPartition(partitionId).toString();
    }

    public static boolean isPartition(String topic) {
        return TopicName.get(topic).isPartitioned();
    }

    /** Merge the same topics or partitions into one topic. */
    public static List<String> distinctTopics(List<String> topics) {
        Set<String> fullTopics = new HashSet<>();
        Map<String, Set<Integer>> partitionedTopics = new HashMap<>();

        for (String topic : topics) {
            TopicName topicName = TopicName.get(topic);
            String partitionedTopicName = topicName.getPartitionedTopicName();

            if (!topicName.isPartitioned()) {
                fullTopics.add(partitionedTopicName);
                partitionedTopics.remove(partitionedTopicName);
            } else if (!fullTopics.contains(partitionedTopicName)) {
                Set<Integer> partitionIds =
                        partitionedTopics.computeIfAbsent(
                                partitionedTopicName, k -> new HashSet<>());
                partitionIds.add(topicName.getPartitionIndex());
            }
        }

        ImmutableList.Builder<String> builder = ImmutableList.<String>builder().addAll(fullTopics);

        for (Map.Entry<String, Set<Integer>> topicSet : partitionedTopics.entrySet()) {
            String topicName = topicSet.getKey();
            for (Integer partitionId : topicSet.getValue()) {
                builder.add(topicNameWithPartition(topicName, partitionId));
            }
        }

        return builder.build();
    }

    /**
     * This method is refactored from {@code BrokerService} in pulsar-broker which is not available
     * in the Pulsar client. We have to put it here and self-maintained. Since these topic names
     * would never be changed for backward compatible, we only need to add new topic names after the
     * version bump.
     *
     * @see <a
     *     href="https://github.com/apache/pulsar/blob/b969fe5e56cc768632e52e9534a1e94b75c29be1/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/BrokerService.java#L3315">BrokerService#isSystemTopic</a>
     */
    public static boolean isInternal(String topic) {
        // A topic name instance without partition information.
        String topicName = topicName(topic);
        TopicName topicInstance = TopicName.get(topicName);
        String namespace = topicInstance.getNamespace();

        return isSystemServiceNamespace(namespace) || isSystemTopic(topicInstance);
    }

    /**
     * This method is refactored from {@code NamespaceService} in pulsar-broker which is not
     * available in the Pulsar client. We have to put it here and self-maintained. Since these
     * namespace names would never be changed for backward compatible, we only need to add new
     * namespace names after the version bump.
     *
     * @see <a
     *     href="https://github.com/apache/pulsar/blob/b969fe5e56cc768632e52e9534a1e94b75c29be1/pulsar-broker/src/main/java/org/apache/pulsar/broker/namespace/NamespaceService.java#L1481">NamespaceService#isSystemServiceNamespace</a>
     */
    public static boolean isSystemServiceNamespace(String namespace) {
        return SYSTEM_NAMESPACE.toString().equals(namespace)
                || SLA_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(namespace).matches();
    }
}
