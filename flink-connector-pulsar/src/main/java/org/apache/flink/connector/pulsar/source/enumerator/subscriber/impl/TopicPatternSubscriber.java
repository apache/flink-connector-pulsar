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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isInternal;

/** Subscribe to matching topics based on topic pattern. */
public class TopicPatternSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 3307710093243745104L;

    private final Pattern shortenedPattern;
    private final String namespace;
    private final RegexSubscriptionMode subscriptionMode;

    public TopicPatternSubscriber(Pattern topicPattern, RegexSubscriptionMode subscriptionMode) {
        this.subscriptionMode = subscriptionMode;
        String pattern = topicPattern.toString();
        this.shortenedPattern =
                pattern.contains("://") ? Pattern.compile(pattern.split("://")[1]) : topicPattern;

        // Extract the namespace from topic pattern regex.
        // If no namespace provided in the regex, we would directly use "default" as the namespace.
        TopicName destination = TopicName.get(topicPattern.pattern());
        NamespaceName namespaceName = destination.getNamespaceObject();
        this.namespace = namespaceName.toString();
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            RangeGenerator generator, int parallelism) throws Exception {
        List<String> topics = admin.namespaces().getTopics(namespace);
        List<String> results = new ArrayList<>(topics.size());

        for (String topic : topics) {
            if (matchesSubscriptionMode(topic)
                    && !isInternal(topic)
                    && matchesTopicPattern(topic)) {
                results.add(topic);
            }
        }
        return createTopicPartitions(results, generator, parallelism);
    }

    /**
     * If the topic matches 'topicsPattern'. This method is in the PulsarClient, and it's removed
     * since 2.11.0 release. We keep the method here.
     */
    private boolean matchesTopicPattern(String topic) {
        String shortenedTopic = topic.split("://")[1];
        return shortenedPattern.matcher(shortenedTopic).matches();
    }

    /**
     * Filter the topic by regex subscription mode. This logic is the same as pulsar consumer's
     * regex subscription.
     */
    private boolean matchesSubscriptionMode(String topic) {
        TopicName topicName = TopicName.get(topic);
        // Filter the topic persistence.
        switch (subscriptionMode) {
            case PersistentOnly:
                return topicName.isPersistent();
            case NonPersistentOnly:
                return !topicName.isPersistent();
            case AllTopics:
                return true;
            default:
                throw new IllegalArgumentException(
                        "We don't support such subscription mode " + subscriptionMode);
        }
    }
}
