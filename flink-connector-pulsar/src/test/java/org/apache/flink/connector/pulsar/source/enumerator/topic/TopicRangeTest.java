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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MAX_RANGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TopicRange}. */
class TopicRangeTest {

    @Test
    void topicRangeIsSerializable() throws Exception {
        final TopicRange range = new TopicRange(1, 5);
        final TopicRange cloneRange = InstantiationUtil.clone(range);
        assertThat(cloneRange).isEqualTo(range);
    }

    @Test
    void negativeStart() {
        assertThatThrownBy(() -> new TopicRange(-1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void endBelowTheMaximum() {
        assertThatCode(() -> new TopicRange(1, MAX_RANGE - 1)).doesNotThrowAnyException();
    }

    @Test
    void endOnTheMaximum() {
        assertThatCode(() -> new TopicRange(1, MAX_RANGE)).doesNotThrowAnyException();
    }

    @Test
    void endAboveTheMaximum() {
        assertThatThrownBy(() -> new TopicRange(1, MAX_RANGE + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
