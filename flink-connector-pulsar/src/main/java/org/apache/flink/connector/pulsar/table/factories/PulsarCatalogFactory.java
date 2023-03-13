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

package org.apache.flink.connector.pulsar.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalog;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions;
import org.apache.flink.connector.pulsar.table.catalog.config.CatalogConfiguration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import java.util.Set;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RATES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RETRIES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_WAIT_MILLIS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTO_CERT_REFRESH_TIME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONCURRENT_LOOKUP_REQUEST;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECTIONS_PER_BROKER;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECTION_MAX_IDLE_SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECTION_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECT_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_DNS_LOOKUP_BIND_ADDRESS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ENABLE_BUSY_WAIT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ENABLE_TRANSACTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_KEEP_ALIVE_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_LISTENER_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_LOOKUP_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_BACKOFF_INTERVAL_NANOS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_LOOKUP_REDIRECTS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_LOOKUP_REQUEST;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MEMORY_LIMIT_BYTES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_NUM_IO_THREADS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_NUM_LISTENER_THREADS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_OPERATION_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_PROXY_PROTOCOL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_PROXY_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_READ_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_REQUEST_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_REQUEST_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SOCKS5_PROXY_ADDRESS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SOCKS5_PROXY_PASSWORD;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SOCKS5_PROXY_USERNAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SSL_PROVIDER;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_CERTIFICATE_FILE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_CIPHERS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_KEY_FILE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_KEY_STORE_PASSWORD;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_KEY_STORE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_KEY_STORE_TYPE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_PROTOCOLS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_STORE_PASSWORD;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_STORE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_STORE_TYPE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_USE_KEY_STORE_TLS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_USE_TCP_NO_DELAY;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_DEFAULT_PARTITION_SIZE;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_DEFAULT_SCHEMA_TYPE;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_HIDDEN_TENANT_PATTERN;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_MANAGED_TENANT;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_VISIBLE_TENANT_PATTERN;
import static org.apache.flink.connector.pulsar.table.catalog.config.PulsarCatalogConfigUtils.createConfiguration;

/** Catalog factory for {@link PulsarCatalog}. */
@PublicEvolving
public class PulsarCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return PulsarCatalogOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return ImmutableSet.<ConfigOption<?>>builder()
                .add(PULSAR_SERVICE_URL)
                .add(PULSAR_ADMIN_URL)
                .build();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        ImmutableSet.Builder<ConfigOption<?>> builder = ImmutableSet.builder();

        // Add the catalog-specified config options.
        builder.add(PULSAR_CATALOG_DEFAULT_DATABASE)
                .add(PULSAR_CATALOG_MANAGED_TENANT)
                .add(PULSAR_CATALOG_HIDDEN_TENANT_PATTERN)
                .add(PULSAR_CATALOG_VISIBLE_TENANT_PATTERN)
                .add(PULSAR_CATALOG_DEFAULT_SCHEMA_TYPE)
                .add(PULSAR_CATALOG_DEFAULT_PARTITION_SIZE);

        // Add all the common config options in PulsarOptions.
        // Some options are useless now and only used in the Pulsar client.
        // We add them here for using only the Pulsar client in the future.
        builder.add(PULSAR_AUTH_PLUGIN_CLASS_NAME)
                .add(PULSAR_AUTH_PARAMS)
                .add(PULSAR_AUTH_PARAM_MAP)
                .add(PULSAR_OPERATION_TIMEOUT_MS)
                .add(PULSAR_LOOKUP_TIMEOUT_MS)
                .add(PULSAR_STATS_INTERVAL_SECONDS)
                .add(PULSAR_NUM_IO_THREADS)
                .add(PULSAR_NUM_LISTENER_THREADS)
                .add(PULSAR_CONNECTIONS_PER_BROKER)
                .add(PULSAR_CONNECTION_MAX_IDLE_SECONDS)
                .add(PULSAR_USE_TCP_NO_DELAY)
                .add(PULSAR_TLS_KEY_FILE_PATH)
                .add(PULSAR_TLS_CERTIFICATE_FILE_PATH)
                .add(PULSAR_TLS_TRUST_CERTS_FILE_PATH)
                .add(PULSAR_TLS_ALLOW_INSECURE_CONNECTION)
                .add(PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE)
                .add(PULSAR_CONCURRENT_LOOKUP_REQUEST)
                .add(PULSAR_MAX_LOOKUP_REQUEST)
                .add(PULSAR_MAX_LOOKUP_REDIRECTS)
                .add(PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION)
                .add(PULSAR_KEEP_ALIVE_INTERVAL_SECONDS)
                .add(PULSAR_CONNECTION_TIMEOUT_MS)
                .add(PULSAR_REQUEST_TIMEOUT_MS)
                .add(PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS)
                .add(PULSAR_MAX_BACKOFF_INTERVAL_NANOS)
                .add(PULSAR_ENABLE_BUSY_WAIT)
                .add(PULSAR_LISTENER_NAME)
                .add(PULSAR_USE_KEY_STORE_TLS)
                .add(PULSAR_SSL_PROVIDER)
                .add(PULSAR_TLS_KEY_STORE_TYPE)
                .add(PULSAR_TLS_KEY_STORE_PATH)
                .add(PULSAR_TLS_KEY_STORE_PASSWORD)
                .add(PULSAR_TLS_TRUST_STORE_TYPE)
                .add(PULSAR_TLS_TRUST_STORE_PATH)
                .add(PULSAR_TLS_TRUST_STORE_PASSWORD)
                .add(PULSAR_TLS_CIPHERS)
                .add(PULSAR_TLS_PROTOCOLS)
                .add(PULSAR_MEMORY_LIMIT_BYTES)
                .add(PULSAR_PROXY_SERVICE_URL)
                .add(PULSAR_PROXY_PROTOCOL)
                .add(PULSAR_ENABLE_TRANSACTION)
                .add(PULSAR_DNS_LOOKUP_BIND_ADDRESS)
                .add(PULSAR_SOCKS5_PROXY_ADDRESS)
                .add(PULSAR_SOCKS5_PROXY_USERNAME)
                .add(PULSAR_SOCKS5_PROXY_PASSWORD)
                .add(PULSAR_CONNECT_TIMEOUT)
                .add(PULSAR_READ_TIMEOUT)
                .add(PULSAR_REQUEST_TIMEOUT)
                .add(PULSAR_AUTO_CERT_REFRESH_TIME)
                .add(PULSAR_ADMIN_REQUEST_RETRIES)
                .add(PULSAR_ADMIN_REQUEST_WAIT_MILLIS)
                .add(PULSAR_ADMIN_REQUEST_RATES);

        return builder.build();
    }

    @Override
    public Catalog createCatalog(Context context) {
        CatalogConfiguration configuration = createConfiguration(this, context);
        return new PulsarCatalog(context.getName(), configuration);
    }
}
