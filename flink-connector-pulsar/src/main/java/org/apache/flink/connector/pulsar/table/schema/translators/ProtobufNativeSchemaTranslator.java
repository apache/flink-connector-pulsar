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

package org.apache.flink.connector.pulsar.table.schema.translators;

import org.apache.flink.connector.pulsar.table.schema.SchemaTranslator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.ArrayList;
import java.util.List;

/** The translator for Pulsar's {@link ProtobufNativeSchema} schema. */
public class ProtobufNativeSchemaTranslator implements SchemaTranslator {

    @Override
    public Schema toSchema(SchemaInfo info) {
        Descriptor descriptor = ProtobufNativeSchemaUtils.deserialize(info.getSchema());
        DataType dataType = descriptorToDataType(descriptor);

        return Schema.newBuilder().fromRowDataType(dataType).build();
    }

    public static DataType descriptorToDataType(Descriptor descriptor) {
        List<Field> fields = new ArrayList<>();
        List<FieldDescriptor> protoFields = descriptor.getFields();

        for (FieldDescriptor fieldDescriptor : protoFields) {
            DataType fieldType = fieldDescriptorToDataType(fieldDescriptor);
            fields.add(DataTypes.FIELD(fieldDescriptor.getName(), fieldType));
        }

        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No FieldDescriptors found");
        }

        return DataTypes.ROW(fields.toArray(new Field[0]));
    }

    private static DataType fieldDescriptorToDataType(FieldDescriptor field) {
        FieldDescriptor.JavaType type = field.getJavaType();
        DataType dataType;
        switch (type) {
            case BOOLEAN:
                dataType = DataTypes.BOOLEAN();
                break;
            case BYTE_STRING:
                dataType = DataTypes.BYTES();
                break;
            case DOUBLE:
                dataType = DataTypes.DOUBLE();
                break;
            case ENUM:
            case STRING:
                dataType = DataTypes.STRING();
                break;
            case FLOAT:
                dataType = DataTypes.FLOAT();
                break;
            case INT:
                dataType = DataTypes.INT();
                break;
            case LONG:
                dataType = DataTypes.BIGINT();
                break;
            case MESSAGE:
                Descriptors.Descriptor msg = field.getMessageType();
                if (field.isMapField()) {
                    // map
                    DataType key = fieldDescriptorToDataType(msg.findFieldByName("key"));
                    DataType value = fieldDescriptorToDataType(msg.findFieldByName("value"));
                    dataType = DataTypes.MAP(key, value);
                } else {
                    // row
                    dataType = descriptorToDataType(field.getMessageType());
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown type: " + type + " for FieldDescriptor: " + field);
        }

        // list
        if (field.isRepeated() && !field.isMapField()) {
            return DataTypes.ARRAY(dataType);
        }

        // single value
        return dataType;
    }

    @Override
    public SchemaInfo toSchemaInfo(ResolvedSchema schema) {
        throw new UnsupportedOperationException(
                "Pulsar protobuf schema can't be generated from Flink schema.");
    }

    @Override
    public SchemaType schemaType() {
        return SchemaType.PROTOBUF_NATIVE;
    }
}
