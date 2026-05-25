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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TableDataTypeUtils}. */
class TableDataTypeUtilsTest {

    @Test
    void testStripRowPrefix() {
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("prefix_name", DataTypes.STRING()),
                        DataTypes.FIELD("prefix_age", DataTypes.INT()),
                        DataTypes.FIELD("other_field", DataTypes.STRING()));

        DataType result = TableDataTypeUtils.stripRowPrefix(dataType, "prefix_");

        RowType resultRowType = (RowType) result.getLogicalType();
        assertThat(resultRowType.getFieldNames()).containsExactly("name", "age", "other_field");
    }

    @Test
    void testStripRowPrefixNoMatch() {
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()));

        DataType result = TableDataTypeUtils.stripRowPrefix(dataType, "prefix_");

        RowType resultRowType = (RowType) result.getLogicalType();
        assertThat(resultRowType.getFieldNames()).containsExactly("name", "age");
    }

    @Test
    void testStripRowPrefixThrowsForNonRowType() {
        DataType dataType = DataTypes.INT();

        assertThatThrownBy(() -> TableDataTypeUtils.stripRowPrefix(dataType, "prefix_"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row data type expected.");
    }

    @Test
    void testRenameRowFields() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("old_a", new VarCharType()),
                                new RowType.RowField("old_b", new IntType())));

        RowType result =
                TableDataTypeUtils.renameRowFields(rowType, Arrays.asList("new_a", "new_b"));

        assertThat(result.getFieldNames()).containsExactly("new_a", "new_b");
        assertThat(result.getTypeAt(0)).isEqualTo(new VarCharType());
        assertThat(result.getTypeAt(1)).isEqualTo(new IntType());
    }

    @Test
    void testRenameRowFieldsPreservesNullability() {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new RowType.RowField("a", new VarCharType()),
                                new RowType.RowField("b", new IntType())));

        RowType result = TableDataTypeUtils.renameRowFields(rowType, Arrays.asList("x", "y"));

        assertThat(result.isNullable()).isTrue();
    }

    @Test
    void testRenameRowFieldsThrowsOnCountMismatch() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("a", new VarCharType()),
                                new RowType.RowField("b", new IntType())));

        assertThatThrownBy(
                        () ->
                                TableDataTypeUtils.renameRowFields(
                                        rowType, Collections.singletonList("only_one")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row field count and new field name count must match.");
    }
}
