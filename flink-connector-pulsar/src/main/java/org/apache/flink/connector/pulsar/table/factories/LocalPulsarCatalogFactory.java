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

import java.util.Collections;
import java.util.Set;

/**
 * Catalog factory for {@link PulsarCatalog}. This is a catalog factory for local testing purpose.
 * We will connect to a local Pulsar instance.
 *
 * <p>All the required configurations in {@link PulsarCatalogFactory} are no longer required. And
 * you can't configure these required configurations.
 */
@PublicEvolving
public class LocalPulsarCatalogFactory extends PulsarCatalogFactory {

    @Override
    public String factoryIdentifier() {
        return PulsarCatalogOptions.LOCAL_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // Override all the required options in the default Pulsar catalog.
        return Collections.emptySet();
    }
}
