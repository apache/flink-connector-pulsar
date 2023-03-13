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

package org.apache.flink.connector.pulsar.table.catalog.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigBuilder;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigValidator;
import org.apache.flink.connector.pulsar.table.factories.PulsarCatalogFactory;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;
import org.apache.flink.table.factories.FactoryUtil.CatalogFactoryHelper;
import org.apache.flink.table.factories.FactoryUtil.FactoryHelper;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;

/** Create the {@link CatalogConfiguration} from the {@link Context} and {@link CatalogFactory}. */
@Internal
public final class PulsarCatalogConfigUtils {

    public static final PulsarConfigValidator CATALOG_VALIDATOR =
            PulsarConfigValidator.builder()
                    .requiredOption(PULSAR_SERVICE_URL)
                    .requiredOption(PULSAR_ADMIN_URL)
                    .conflictOptions(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP)
                    .build();

    private PulsarCatalogConfigUtils() {
        // No public constructor
    }

    /**
     * A method for creating the config from the catalog factory context. We drop the use of {@link
     * FactoryHelper} because the Pulsar connector has provided the validation.
     */
    public static CatalogConfiguration createConfiguration(
            PulsarCatalogFactory factory, Context context) {
        // Validate the catalog config by using flink's configuration.
        CatalogFactoryHelper helper = new CatalogFactoryHelper(factory, context);
        helper.validate();

        // Validate the conflict config options which are not supported in Flink catalog helper.
        PulsarConfigBuilder builder = new PulsarConfigBuilder(context.getOptions());
        return builder.build(CATALOG_VALIDATOR, CatalogConfiguration::new);
    }
}
