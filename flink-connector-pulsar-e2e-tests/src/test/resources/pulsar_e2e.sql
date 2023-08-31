-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE JsonTable (
  rowtime TIMESTAMP(3),
  `user` STRING,
  event ROW<`type` STRING, message STRING>,
  WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND
) WITH (
  'connector' = '$PULSAR_IDENTIFIER',
  'topics' = '$TOPIC_JSON_NAME',
  'service-url' = '$PULSAR_SERVICE_URL',
  'admin-url' = '$PULSAR_ADMIN_URL',
  'format' = 'json'
);


CREATE TABLE AvroTable (
  event_timestamp STRING,
  `user` STRING,
  message STRING,
  duplicate_count BIGINT
) WITH (
  'connector' = '$PULSAR_IDENTIFIER',
  'topics' = '$TOPIC_AVRO_NAME',
  'service-url' = '$PULSAR_SERVICE_URL',
  'admin-url' = '$PULSAR_ADMIN_URL',
  'format' = 'avro'
);

CREATE TABLE ResultCsvTable (
  event_timestamp STRING,
  `user` STRING,
  message STRING,
  duplicate_count BIGINT,
  constant STRING
) WITH (
  'connector' = '$PULSAR_IDENTIFIER',
  'topics' = '$TOPIC_RESULT_NAME',
  'service-url' = '$PULSAR_SERVICE_URL',
  'admin-url' = '$PULSAR_ADMIN_URL',
  'format' = 'csv'
);

CREATE FUNCTION RegReplace AS 'org.apache.flink.table.toolbox.StringRegexReplaceFunction';

INSERT INTO AvroTable
SELECT
  CAST(TUMBLE_START(rowtime, INTERVAL '1' HOUR) AS VARCHAR) AS event_timestamp,
  `user`,
  RegReplace(event.message, ' is ', ' was ') AS message,
  COUNT(*) AS duplicate_count
FROM JsonTable
WHERE `user` IS NOT NULL
GROUP BY
  `user`,
  event.message,
  TUMBLE(rowtime, INTERVAL '1' HOUR);

INSERT INTO ResultCsvTable
SELECT AvroTable.*, RegReplace('Test constant folding.', 'Test', 'Success') AS constant
FROM AvroTable;
