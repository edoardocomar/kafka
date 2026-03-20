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
package org.apache.kafka.clients.security;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_ADMIN;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_ADMIN_PASSWORD;
import static org.apache.kafka.common.test.api.TestKitDefaults.BROKER_ID_OFFSET;
import static org.apache.kafka.common.test.api.TestKitDefaults.CONTROLLER_ID_OFFSET;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG;
import static org.apache.kafka.network.SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG;
import static org.apache.kafka.network.SocketServerConfigs.LISTENERS_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test that verifies a KRaft cluster with 3 listeners:
 * <ul>
 *   <li>EXTERNAL — SASL_PLAINTEXT with SASL PLAIN mechanism (client-facing)</li>
 *   <li>INTERBROKER — PLAINTEXT (inter-broker communication)</li>
 *   <li>CONTROLLER — PLAINTEXT (controller communication)</li>
 * </ul>
 */
public class SaslPlainMultiListenerTest {

    private static final String ALICE = "alice";
    private static final String ALICE_PASSWORD = "aaapwd";
    private static final String BOB = "bob";
    private static final String BOB_PASSWORD = "bbbpwd";

    // Server-side JAAS: defines alice and bob plus the framework's default admin user
    // (needed by the test framework's internal admin client for cluster operations)
    private static final String SERVER_JAAS_CONFIG =
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "user_alice=\"" + ALICE_PASSWORD + "\" "
            + "user_bob=\"" + BOB_PASSWORD + "\" "
            + "user_" + KAFKA_PLAIN_ADMIN + "=\"" + KAFKA_PLAIN_ADMIN_PASSWORD + "\";";

    private static String clientJaasConfig(String username, String password) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"" + username + "\" "
            + "password=\"" + password + "\";";
    }

    static List<ClusterConfig> clusterConfig() {
        Map<String, String> serverProperties = new LinkedHashMap<>();
        serverProperties.put(LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
            "EXTERNAL:SASL_PLAINTEXT,INTERBROKER:PLAINTEXT,CONTROLLER:PLAINTEXT");
        serverProperties.put(INTER_BROKER_LISTENER_NAME_CONFIG, "INTERBROKER");
        serverProperties.put(SASL_ENABLED_MECHANISMS_CONFIG, "PLAIN");
        serverProperties.put("listener.name.external.plain.sasl.jaas.config", SERVER_JAAS_CONFIG);
        // Internal topic partitions: 3
        serverProperties.put(OFFSETS_TOPIC_PARTITIONS_CONFIG, "3");
        serverProperties.put("transaction.state.log.num.partitions", "3");
        serverProperties.put("share.coordinator.state.topic.num.partitions", "3");
        // Replication factors: 2
        serverProperties.put(OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
        serverProperties.put("transaction.state.log.replication.factor", "2");
        serverProperties.put("transaction.state.log.min.isr", "1");
        serverProperties.put("share.coordinator.state.topic.replication.factor", "2");
        serverProperties.put("share.coordinator.state.topic.min.isr", "1");
        serverProperties.put("default.replication.factor", "2");
        serverProperties.put(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true");

        Map<String, String> brokerListeners = Map.of(
            LISTENERS_CONFIG, "EXTERNAL://localhost:0,INTERBROKER://localhost:0"
        );
        Map<String, String> controllerListeners = Map.of(
            LISTENERS_CONFIG, "CONTROLLER://localhost:0"
        );

        // Broker IDs: 0, 1  —  Controller IDs: 3000, 3001
        Map<Integer, Map<String, String>> perServerProperties = Map.of(
            BROKER_ID_OFFSET, brokerListeners,
            BROKER_ID_OFFSET + 1, brokerListeners,
            BROKER_ID_OFFSET + CONTROLLER_ID_OFFSET, controllerListeners,
            BROKER_ID_OFFSET + CONTROLLER_ID_OFFSET + 1, controllerListeners
        );

        return List.of(
            ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(2)
                .setControllers(2)
                .setBrokerSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
                .setControllerSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .setServerProperties(serverProperties)
                .setPerServerProperties(perServerProperties)
                .build()
        );
    }

    @ClusterTemplate("clusterConfig")
    public void testProduceConsumeWithSaslExternalListener(ClusterInstance cluster) throws InterruptedException, ExecutionException {
        String topic = "test-topic";
        cluster.createTopic(topic, 1, (short) 2);

        // Producer authenticates as alice
        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of(
                SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
                SASL_MECHANISM, "PLAIN",
                SASL_JAAS_CONFIG, clientJaasConfig(ALICE, ALICE_PASSWORD)));
             // Consumer authenticates as bob
             Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(
                SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
                SASL_MECHANISM, "PLAIN",
                SASL_JAAS_CONFIG, clientJaasConfig(BOB, BOB_PASSWORD)))) {

            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes())).get();

            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(30));
            assertEquals(1, records.count());
            assertEquals("value", new String(records.iterator().next().value()));
        }
    }
}
