---
title: Pulsar
weight: 9
type: docs
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Pulsar SQL Connector

{{< label "Scan Source: Unbounded" >}}
{{< label "Scan Source: Bounded" >}}
{{< label "Sink: Streaming Append Mode" >}}

The Pulsar connector allows for reading data from and writing data into Pulsar topics.

Dependencies
------------

The Pulsar connector is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

How to create a Pulsar table
----------------

The example below shows how to create a Pulsar table:

```sql
CREATE TABLE PulsarTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'pulsar',
  'topics' = 'user_behavior',
  'service-url' = 'pulsar://my-broker.com:6650',
)
```

Connector Options
----------------

{{< generated/pulsar_table_configuration >}}
