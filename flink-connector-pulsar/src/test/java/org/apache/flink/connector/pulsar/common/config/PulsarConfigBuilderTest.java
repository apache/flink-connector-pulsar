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

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarConfigBuilder}. */
class PulsarConfigBuilderTest {

    @Test
    void canNotSetSameOptionTwiceWithDifferentValue() {
        ConfigOption<String> option = ConfigOptions.key("some.key").stringType().noDefaultValue();
        PulsarConfigBuilder builder = new PulsarConfigBuilder();
        builder.set(option, "value1");

        assertThatCode(() -> builder.set(option, "value1")).doesNotThrowAnyException();

        assertThatThrownBy(() -> builder.set(option, "value2"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void setConfigurationCanNotOverrideExistedKeysWithNewValue() {
        ConfigOption<String> option = ConfigOptions.key("string.k1").stringType().noDefaultValue();
        PulsarConfigBuilder builder = new PulsarConfigBuilder();

        Configuration configuration = new Configuration();
        configuration.set(option, "value1");

        builder.set(option, "value1");
        assertThatCode(() -> builder.set(configuration)).doesNotThrowAnyException();

        configuration.set(option, "value2");
        assertThatThrownBy(() -> builder.set(configuration))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void setPropertiesCanNotOverrideExistedKeysWithNewValueAndSupportTypeConversion() {
        ConfigOption<Integer> option = ConfigOptions.key("int.type").intType().defaultValue(3);
        PulsarConfigBuilder builder = new PulsarConfigBuilder();

        Properties properties = new Properties();
        properties.put("int.type", "6");

        assertThatCode(() -> builder.set(properties)).doesNotThrowAnyException();

        properties.put("int.type", "1");
        assertThatThrownBy(() -> builder.set(properties))
                .isInstanceOf(IllegalArgumentException.class);

        Integer value = builder.get(option);
        assertThat(value).isEqualTo(6);
    }
}
