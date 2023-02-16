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

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isInternal;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Subscribe to matching topics based on the topic pattern. */
public class TopicPatternSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 3307710093243745104L;

    private final Pattern shortenedPattern;
    private final String namespace;
    private final Mode subscriptionMode;

    public TopicPatternSubscriber(Pattern topicPattern, RegexSubscriptionMode subscriptionMode) {
        String pattern = topicPattern.toString();
        this.shortenedPattern =
                pattern.contains("://") ? Pattern.compile(pattern.split("://")[1]) : topicPattern;

        // Extract the namespace from topic pattern regex.
        // If no namespace provided in the regex, we would directly use "default" as the namespace.
        TopicName destination = TopicName.get(topicPattern.pattern());
        NamespaceName namespaceName = destination.getNamespaceObject();
        this.namespace = namespaceName.toString();
        this.subscriptionMode = convertRegexSubscriptionMode(subscriptionMode);
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            RangeGenerator generator, int parallelism) throws Exception {
        Set<String> topics = queryTopicsByInternalProtocols();
        return createTopicPartitions(topics, generator, parallelism);
    }

    /**
     * We reuse this internal protocol in the Pulsar client for achieving the same behavior as
     * directly using the client to consume the topic pattern.
     */
    private Set<String> queryTopicsByInternalProtocols() throws PulsarClientException {
        checkNotNull(client, "This subscriber doesn't initialize properly.");

        LookupService lookupService = ((PulsarClientImpl) client).getLookup();
        NamespaceName namespaceName = NamespaceName.get(namespace);
        try {
            // Pulsar 2.11.0 can filter regular expression on broker, but it has a bug which can
            // only be used for wildcard filtering.
            String queryPattern = shortenedPattern.toString();
            if (!queryPattern.endsWith(".*")) {
                queryPattern = null;
            }

            GetTopicsResult topicsResult =
                    lookupService
                            .getTopicsUnderNamespace(
                                    namespaceName, subscriptionMode, queryPattern, null)
                            .get();
            List<String> topics = topicsResult.getTopics();
            Set<String> results = new HashSet<>(topics.size());

            // The regular expression filter may not be enabled in broker.
            // Add the filter here if the result is not filtered.
            for (String topic : topics) {
                if (!isInternal(topic)
                        && (topicsResult.isFiltered() || matchesTopicPattern(topic))) {
                    results.add(topic);
                }
            }

            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    /**
     * If the topic matches 'topicsPattern'. This method is in the PulsarClient, and it's removed
     * since 2.11.0 release. We keep the method here. It's copied from {@link
     * TopicList#filterTopics(List, String)}.
     */
    private boolean matchesTopicPattern(String topic) {
        String shortenedTopic = topic.split("://")[1];
        return shortenedPattern.matcher(shortenedTopic).matches();
    }

    /** Convert the subscription mode into the internal binary protocol. */
    private Mode convertRegexSubscriptionMode(RegexSubscriptionMode subscriptionMode) {
        switch (subscriptionMode) {
            case AllTopics:
                return Mode.ALL;
            case PersistentOnly:
                return Mode.PERSISTENT;
            case NonPersistentOnly:
                return Mode.NON_PERSISTENT;
            default:
                throw new IllegalArgumentException(
                        "We don't support such subscription mode " + subscriptionMode);
        }
    }
}
