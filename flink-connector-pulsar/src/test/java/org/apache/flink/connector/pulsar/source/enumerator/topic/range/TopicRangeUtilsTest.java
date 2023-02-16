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

package org.apache.flink.connector.pulsar.source.enumerator.topic.range;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.isFullTopicRanges;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.validateTopicRanges;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link TopicRangeUtils}. */
class TopicRangeUtilsTest {

    @Test
    void testValidateTopicRanges() {
        List<TopicRange> ranges1 = Arrays.asList(new TopicRange(1, 2), new TopicRange(2, 3));
        assertThatThrownBy(() -> validateTopicRanges(ranges1))
                .isInstanceOf(IllegalArgumentException.class);

        List<TopicRange> ranges2 = Arrays.asList(new TopicRange(1, 14), new TopicRange(2, 5));
        assertThatThrownBy(() -> validateTopicRanges(ranges2))
                .isInstanceOf(IllegalArgumentException.class);

        List<TopicRange> ranges3 = Arrays.asList(new TopicRange(1, 14), new TopicRange(5, 30));
        assertThatThrownBy(() -> validateTopicRanges(ranges3))
                .isInstanceOf(IllegalArgumentException.class);

        List<TopicRange> ranges4 = Arrays.asList(new TopicRange(1, 14), new TopicRange(15, 30));
        assertThatCode(() -> validateTopicRanges(ranges4)).doesNotThrowAnyException();
    }

    @Test
    void testIsFullTopicRanges() {
        List<TopicRange> ranges1 =
                Arrays.asList(
                        new TopicRange(16384, 32767),
                        new TopicRange(0, 16383),
                        new TopicRange(32768, 49151),
                        new TopicRange(49152, 65535));
        assertThat(isFullTopicRanges(ranges1)).isTrue();

        List<TopicRange> ranges2 =
                Arrays.asList(
                        new TopicRange(32768, 49151),
                        new TopicRange(0, 16383),
                        new TopicRange(16384, 32767),
                        new TopicRange(49152, 65531));
        assertThat(isFullTopicRanges(ranges2)).isFalse();

        List<TopicRange> ranges3 =
                Arrays.asList(
                        new TopicRange(33, 16383),
                        new TopicRange(32768, 49151),
                        new TopicRange(16384, 32767),
                        new TopicRange(49152, 65535));
        assertThat(isFullTopicRanges(ranges3)).isFalse();

        List<TopicRange> ranges4 = Collections.singletonList(TopicRange.createFullRange());
        assertThat(isFullTopicRanges(ranges4)).isTrue();
    }
}
