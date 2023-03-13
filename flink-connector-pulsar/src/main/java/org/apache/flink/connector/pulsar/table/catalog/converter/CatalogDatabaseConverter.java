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

package org.apache.flink.connector.pulsar.table.catalog.converter;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;

import org.apache.flink.shaded.guava30.com.google.common.base.Converter;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A converter for converting the {@link CatalogDatabase} to Pulsar namespace properties and
 * backward converting.
 */
public class CatalogDatabaseConverter extends Converter<CatalogDatabase, Map<String, String>> {

    private static final String COMMENT_KEY = "__flink_database_comment";
    private static final String DESCRIPTION_KEY = "__flink_database_description";
    private static final String DETAILED_DESCRIPTION_KEY = "__flink_database_detailed_description";

    @Override
    protected Map<String, String> doForward(CatalogDatabase catalogDatabase) {
        Map<String, String> properties =
                Optional.ofNullable(catalogDatabase.getProperties())
                        .map(HashMap::new)
                        .orElseGet(HashMap::new);

        Optional.ofNullable(catalogDatabase.getComment())
                .filter(c -> !c.isEmpty())
                .ifPresent(comment -> properties.put(COMMENT_KEY, comment));

        catalogDatabase
                .getDescription()
                .filter(c -> !c.isEmpty())
                .ifPresent(description -> properties.put(DESCRIPTION_KEY, description));

        catalogDatabase
                .getDetailedDescription()
                .filter(c -> !c.isEmpty())
                .ifPresent(detailed -> properties.put(DETAILED_DESCRIPTION_KEY, detailed));

        return properties;
    }

    @Override
    protected CatalogDatabase doBackward(Map<String, String> properties) {
        Map<String, String> prop = new HashMap<>(properties);
        String comment = prop.remove(COMMENT_KEY);
        String description = prop.remove(DESCRIPTION_KEY);
        String detailedDescription = prop.remove(DETAILED_DESCRIPTION_KEY);

        return new PulsarCatalogDatabase(prop, comment, description, detailedDescription);
    }

    /**
     * An implementation of {@link CatalogDatabase}. We didn't use the default {@link
     * CatalogDatabaseImpl} for adding the description support.
     */
    public static class PulsarCatalogDatabase implements CatalogDatabase {

        private final Map<String, String> properties;
        private final String comment;
        private final String description;
        private final String detailedDescription;

        public PulsarCatalogDatabase(
                Map<String, String> properties,
                @Nullable String comment,
                @Nullable String description,
                @Nullable String detailedDescription) {
            this.properties = checkNotNull(properties, "properties cannot be null");
            this.comment = comment;
            this.description = description;
            this.detailedDescription = detailedDescription;
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public String getComment() {
            return comment;
        }

        @Override
        public CatalogDatabase copy() {
            return copy(getProperties());
        }

        @Override
        public CatalogDatabase copy(Map<String, String> newProperties) {
            Map<String, String> prop = new HashMap<>(newProperties);
            return new PulsarCatalogDatabase(prop, comment, description, detailedDescription);
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.ofNullable(description);
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.ofNullable(detailedDescription);
        }
    }
}
