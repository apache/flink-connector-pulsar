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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility methods for working with table {@link DataType}s.
 *
 * <p>This class contains methods that were removed from Flink's {@code DataTypeUtils} in Flink 2.x.
 * They are copied here to maintain the connector's functionality.
 */
@Internal
public class TableDataTypeUtils {

    private TableDataTypeUtils() {}

    /**
     * Removes a prefix from all field names in a row data type.
     *
     * @param dataType The row data type whose field names should be stripped.
     * @param prefix The prefix to remove from each field name.
     * @return A new data type with the prefix removed from field names.
     */
    public static DataType stripRowPrefix(DataType dataType, String prefix) {
        if (!dataType.getLogicalType().is(LogicalTypeRoot.ROW)) {
            throw new IllegalArgumentException("Row data type expected.");
        }

        final RowType rowType = (RowType) dataType.getLogicalType();
        final List<String> newFieldNames =
                rowType.getFieldNames().stream()
                        .map(
                                s -> {
                                    if (s.startsWith(prefix)) {
                                        return s.substring(prefix.length());
                                    }
                                    return s;
                                })
                        .collect(Collectors.toList());
        final LogicalType newRowType = renameRowFields(rowType, newFieldNames);
        return new FieldsDataType(
                newRowType, dataType.getConversionClass(), dataType.getChildren());
    }

    /**
     * Renames the fields in a {@link RowType}.
     *
     * @param rowType The row type to rename fields in.
     * @param newFieldNames The new field names to use.
     * @return A new row type with the renamed fields.
     */
    public static RowType renameRowFields(RowType rowType, List<String> newFieldNames) {
        if (rowType.getFieldCount() != newFieldNames.size()) {
            throw new IllegalArgumentException(
                    "Row field count and new field name count must match.");
        }

        final List<RowType.RowField> newFields =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                i -> {
                                    RowType.RowField oldField = rowType.getFields().get(i);
                                    return new RowType.RowField(
                                            newFieldNames.get(i),
                                            oldField.getType(),
                                            oldField.getDescription().orElse(null));
                                })
                        .collect(Collectors.toList());

        return new RowType(rowType.isNullable(), newFields);
    }
}
