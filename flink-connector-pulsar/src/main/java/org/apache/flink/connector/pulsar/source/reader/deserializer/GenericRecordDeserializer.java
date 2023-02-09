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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import org.apache.pulsar.client.api.schema.GenericRecord;

import java.io.Serializable;

/**
 * This is deserializer for deserialize the Pulsar's {@link GenericRecord} in a result and send it
 * to the downstream.
 *
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
@SuppressWarnings("java:S112")
public interface GenericRecordDeserializer<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * GenericRecord is an interface represents a message with schema. It was created from the
     * client with the native object included and the corresponding schema information.
     */
    T deserialize(GenericRecord message) throws Exception;
}
