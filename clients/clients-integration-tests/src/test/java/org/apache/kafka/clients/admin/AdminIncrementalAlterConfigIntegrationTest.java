/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.server.config.QuotaConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AdminIncrementalAlterConfigIntegrationTest {
    private final ClusterInstance cluster;

    public AdminIncrementalAlterConfigIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    /* with Kafka 4.0
    server logs show unexpected failure on server,
    
    ERROR Encountered nonFatalFaultHandler fault: incrementalAlterConfigs: event failed with RuntimeException (treated as UnknownServerException) 
        at epoch 1 in 18143 microseconds. Renouncing leadership and reverting to the last committed offset 17. (org.apache.kafka.common.test.MockFaultHandler:47)
            java.lang.RuntimeException: 'value' field is too long to be serialized
            at org.apache.kafka.common.metadata.ConfigRecord.addSize(ConfigRecord.java:192)
            at org.apache.kafka.common.protocol.Message.size(Message.java:51)
    client gets UnknownServerException

    with Kafka 4.3 client gets InvalidConfigurationException
        java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.InvalidConfigurationException: The configuration value cannot be added because it exceeds the maximum value size of 32767 bytes.
        at java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)
        at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)
        at org.apache.kafka.common.internals.KafkaFutureImpl.get(KafkaFutureImpl.java:155)
        at org.apache.kafka.clients.admin.AdminIncrementalAlterConfigIntegrationTest.testKafka18020(AdminIncrementalAlterConfigIntegrationTest.java:73)

        Caused by:
        org.apache.kafka.common.errors.InvalidConfigurationException: The configuration value cannot be added because it exceeds the maximum value size of 32767 bytes.
     */
    @ClusterTest
    public void testKafka18020() throws Exception {
        final String topicName = "testIncrementalAlterConfigAppendKafka18020";
        try (Admin adminClient = cluster.admin()) {
            adminClient.createTopics(Collections.singleton(new NewTopic(topicName, Optional.empty(), Optional.empty()))).all().get();

            try {
                for (int i = 0; i < 10; i++) {
                    StringBuilder sb = new StringBuilder();
                    for (int j = 1000 * i; j < 1000 * (i + 1); j++) {
                        sb.append(j).append(":").append(j).append(",");
                    }
                    sb.setLength(sb.length() - 1);
                    String throttles = sb.toString(); //"0:0,...,999:999" for i=0; then "1000:1000,...,1999:1999" and so on

                    Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
                    List<AlterConfigOp> ops = new ArrayList<>();
                    ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttles), AlterConfigOp.OpType.APPEND));
                    ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttles), AlterConfigOp.OpType.APPEND));
                    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topicName), ops);

                    System.out.println("i=" + i + " throttles=" + throttles.substring(0, 10) + "..." + throttles.substring(throttles.length() - 10));

                    adminClient.incrementalAlterConfigs(configs).all().get();
                }
            } finally {
                adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
            }
        }
    }
}
