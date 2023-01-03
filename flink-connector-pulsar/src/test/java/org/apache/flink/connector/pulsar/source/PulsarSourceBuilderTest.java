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

package org.apache.flink.connector.pulsar.source;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link PulsarSourceBuilder}. */
class PulsarSourceBuilderTest {

    @Test
    void someSetterMethodCouldOnlyBeCalledOnce() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);
        assertAll(
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setAdminUrl("admin-url2")),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setServiceUrl("service-url2")),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setSubscriptionName("set_subscription_name2")));
    }

    @Test
    void topicPatternAndListCouldChooseOnlyOne() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);
        assertThatThrownBy(() -> builder.setTopicPattern("a-a-a"))
                .isInstanceOf(IllegalStateException.class);
    }

    private void fillRequiredFields(PulsarSourceBuilder<String> builder) {
        builder.setAdminUrl("admin-url");
        builder.setServiceUrl("service-url");
        builder.setSubscriptionName("subscription-name");
        builder.setTopics("topic");
        builder.setDeserializationSchema(pulsarSchema(Schema.STRING));
    }
}
