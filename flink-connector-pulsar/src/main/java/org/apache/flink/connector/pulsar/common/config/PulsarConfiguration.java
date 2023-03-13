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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.UnmodifiableConfiguration;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * An unmodifiable {@link Configuration} for Pulsar. We provide extra methods for building the
 * different Pulsar client instance.
 */
public abstract class PulsarConfiguration extends UnmodifiableConfiguration {
    private static final long serialVersionUID = 3050894147145572345L;

    /**
     * Creates a new PulsarConfiguration, which holds a copy of the given configuration that can't
     * be altered.
     *
     * @param config The configuration with the original contents.
     */
    protected PulsarConfiguration(Configuration config) {
        super(config);
    }

    /** Get an option value from the given config, convert it into a new value instance. */
    public <F, T> T get(ConfigOption<F> option, Function<F, T> convertor) {
        F value = get(option);
        if (value != null) {
            return convertor.apply(value);
        } else {
            return null;
        }
    }

    /** Set the config option's value to a given builder. */
    public <T> void useOption(ConfigOption<T> option, Consumer<T> setter) {
        useOption(option, identity(), setter);
    }

    /**
     * Query the config option's value, convert it into a required type, set it to a given builder.
     */
    public <T, V> void useOption(
            ConfigOption<T> option, Function<T, V> convertor, Consumer<V> setter) {
        if (contains(option) || option.hasDefaultValue()) {
            V value = get(option, convertor);
            setter.accept(value);
        }
    }

    /**
     * Use {@link #getDuration(ConfigOption, TimeUnit)} for supporting two types of config value for
     * a same config option key. We will convert the value into a duration and try to set it.
     */
    public void useDuration(ConfigOption<?> option, TimeUnit timeUnit, Consumer<Duration> setter) {
        Duration duration = getDuration(option, timeUnit);
        if (duration != null) {
            setter.accept(duration);
        }
    }

    /**
     * A lot of time-related configs in Pulsar are defined in long or int format as the millie
     * seconds or other time units. But it would be better to use {@link Duration} format because it
     * would be easily configured in SQL {@code with} clause.
     *
     * <p>So we create this method for supporting {@link Duration} for all the time related options.
     * And we don't need to change the existing config option formats.
     *
     * @return We will return null if the config doesn't contain the given option and the option
     *     doesn't contain any default value.
     */
    @Nullable
    public Duration getDuration(ConfigOption<?> option, TimeUnit timeUnit) {
        if (!contains(option)) {
            if (option.hasDefaultValue()) {
                return convertToDuration(option.defaultValue(), timeUnit);
            }

            return null;
        }

        return convertToDuration(getValue(option), timeUnit);
    }

    /** Convert the configured duration into a numeric value. The duration mustn't be null. */
    public <T> T getDuration(
            ConfigOption<?> option, TimeUnit timeUnit, Function<Duration, T> convertor) {
        Duration duration = getDuration(option, timeUnit);
        return convertor.apply(requireNonNull(duration, option.key() + " is null"));
    }

    private Duration convertToDuration(Object value, TimeUnit timeUnit) {
        try {
            Long time = ConfigurationUtils.convertValue(value, Long.class);
            return createDuration(time, timeUnit);
        } catch (Exception e) {
            // Fallback to a duration converting.
            try {
                return ConfigurationUtils.convertValue(value, Duration.class);
            } catch (Exception ex) {
                // This could ba a normal duration instance.
                return Duration.parse(value.toString());
            }
        }
    }

    private Duration createDuration(long amount, TimeUnit timeUnit) {
        switch (timeUnit) {
            case NANOSECONDS:
                return Duration.ofNanos(amount);
            case MICROSECONDS:
                return Duration.of(amount, ChronoUnit.MICROS);
            case MILLISECONDS:
                return Duration.ofMillis(amount);
            case SECONDS:
                return Duration.ofSeconds(amount);
            case MINUTES:
                return Duration.ofMinutes(amount);
            case HOURS:
                return Duration.ofHours(amount);
            case DAYS:
                return Duration.ofDays(amount);
        }

        throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
    }
}
