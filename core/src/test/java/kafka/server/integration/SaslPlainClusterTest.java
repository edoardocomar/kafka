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
package kafka.server.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SaslPlainClusterTest {
    @ClusterTemplate("config1Broker1Controller")
    public void testSaslPlainLogin(ClusterInstance cluster) throws Exception {
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(ListenerName.normalised("CLIENT")));
        adminConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        adminConfig.put(SaslConfigs.SASL_JAAS_CONFIG, """
            org.apache.kafka.common.security.plain.PlainLoginModule required \
            username="edo"
            password="edo_pwd";
            """);

        Map<String, Object> adminConfigBadPwd = new HashMap<>(adminConfig);
        adminConfigBadPwd.put(SaslConfigs.SASL_JAAS_CONFIG, """
            org.apache.kafka.common.security.plain.PlainLoginModule required \
            username="edo"
            password="bad_pwd";
            """);

        try (Admin admin = cluster.admin(adminConfig); Admin adminBadPwd = cluster.admin(adminConfigBadPwd)) {
            NewTopic topic1 = new NewTopic("topic1", Optional.of(1), Optional.of((short) 1));
            admin.createTopics(List.of(topic1)).all().get();

            try {
                adminBadPwd.createTopics(List.of(topic1)).all().get();
                Assertions.fail("ExecutionException expected");
            } catch (ExecutionException ee) {
                Assertions.assertInstanceOf(SaslAuthenticationException.class, ee.getCause());
                Assertions.assertTrue(ee.getCause().getMessage().contains("Authentication failed: Invalid username or password"),
                        ee.getCause().getMessage());
            }
        }
    }

    @SuppressWarnings("unused")
    static List<ClusterConfig> config1Broker1Controller() {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "INTER_BROKER:PLAINTEXT,CLIENT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
        serverProperties.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "INTER_BROKER");
        serverProperties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        serverProperties.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, DefaultKafkaPrincipalBuilder.class.getName());

        Map<Integer, Map<String, String>> perServerProperties = new HashMap<>();
        Map<String, String> brokerConfig = new HashMap<>();
        brokerConfig.put(SocketServerConfigs.LISTENERS_CONFIG, "INTER_BROKER://localhost:0,CLIENT://localhost:0");
        brokerConfig.put("listener.name.client.sasl.enabled.mechanisms", "PLAIN");
        brokerConfig.put("listener.name.client.plain.sasl.jaas.config",
                """
                org.apache.kafka.common.security.plain.PlainLoginModule required \
                user_edo="edo_pwd";
                """);

        Map<String, String> controllerConfig = new HashMap<>();
        controllerConfig.put(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:0");

        perServerProperties.put(TestKitDefaults.BROKER_ID_OFFSET, brokerConfig);
        perServerProperties.put(TestKitDefaults.CONTROLLER_ID_OFFSET, controllerConfig);

        ClusterConfig.Builder builder = ClusterConfig.defaultBuilder()
                .setTypes(Stream.of(Type.KRAFT).collect(Collectors.toSet()))
                .setBrokers(1)
                .setControllers(1)
                .setBrokerListenerName(ListenerName.normalised("INTER_BROKER"))
                .setControllerListenerName(ListenerName.normalised("CONTROLLER"))
                .setServerProperties(serverProperties)
                .setPerServerProperties(perServerProperties);
        return List.of(builder.build());
    }
}

