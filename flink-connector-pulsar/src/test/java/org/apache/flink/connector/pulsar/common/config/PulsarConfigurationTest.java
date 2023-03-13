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

package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PulsarConfiguration}. */
class PulsarConfigurationTest {

    private static final ConfigOption<Map<String, String>> PROP_OP =
            ConfigOptions.key("some.config").mapType().defaultValue(emptyMap());
    private static final ConfigOption<Long> LONG_OP =
            ConfigOptions.key("some.long").longType().defaultValue(40000L);
    private static final ConfigOption<Long> NULL_LONG_OP =
            ConfigOptions.key("some.long").longType().noDefaultValue();
    private static final ConfigOption<Duration> DURATION_OP =
            ConfigOptions.key("some.duration")
                    .durationType()
                    .defaultValue(Duration.ofMillis(50000));

    @Test
    void pulsarConfigurationCanGetMapWithPrefix() {
        Properties expectProp = new Properties();
        for (int i = 0; i < 10; i++) {
            expectProp.put(randomAlphabetic(10), randomAlphabetic(10));
        }

        Configuration configuration = new Configuration();

        for (String name : expectProp.stringPropertyNames()) {
            configuration.setString(PROP_OP.key() + "." + name, expectProp.getProperty(name));
        }

        TestConfiguration configuration1 = new TestConfiguration(configuration);
        Map<String, String> properties = configuration1.get(PROP_OP);

        assertThat(properties).isEqualTo(expectProp);
    }

    @Test
    void pulsarConfigurationCanGetDuration() {
        // Test default value format.
        Configuration configuration = new Configuration();
        configuration.set(LONG_OP, 300000L);
        configuration.set(DURATION_OP, Duration.ofMillis(100000));
        TestConfiguration configuration1 = new TestConfiguration(configuration);

        Duration duration1 = configuration1.getDuration(LONG_OP, TimeUnit.MILLISECONDS);
        assertThat(duration1).hasMillis(300000);
        Duration duration2 = configuration1.getDuration(DURATION_OP, TimeUnit.MILLISECONDS);
        assertThat(duration2).hasMillis(100000);

        // Test default value.
        TestConfiguration configuration2 = new TestConfiguration(new Configuration());

        Duration duration3 = configuration2.getDuration(LONG_OP, TimeUnit.MILLISECONDS);
        assertThat(duration3).hasMillis(40000);
        Duration duration4 = configuration2.getDuration(DURATION_OP, TimeUnit.MILLISECONDS);
        assertThat(duration4).hasMillis(50000);

        // Test null value.
        TestConfiguration configuration3 = new TestConfiguration(new Configuration());

        Duration duration5 = configuration3.getDuration(NULL_LONG_OP, TimeUnit.MILLISECONDS);
        assertThat(duration5).isNull();

        // Test different value format.
        Configuration configuration4 = new Configuration();
        configuration4.setString(LONG_OP.key(), "10min");
        configuration4.setString(DURATION_OP.key(), "1000");
        TestConfiguration configuration5 = new TestConfiguration(configuration4);

        Duration duration6 = configuration5.getDuration(LONG_OP, TimeUnit.MINUTES);
        assertThat(duration6).hasMinutes(10);
        Duration duration7 = configuration5.getDuration(DURATION_OP, TimeUnit.MINUTES);
        assertThat(duration7).hasMinutes(1000);
    }

    private static final class TestConfiguration extends PulsarConfiguration {
        private static final long serialVersionUID = 944689984000450917L;

        private TestConfiguration(Configuration config) {
            super(config);
        }
    }
}
