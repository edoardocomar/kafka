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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@ClusterTestDefaults(
    types = {Type.KRAFT}
)
public class DemoTelemetryIntegrationTest {
    private final ClusterInstance clusterInstance;

    public DemoTelemetryIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "metric.reporters", value = "kafka.server.integration.DemoTelemetryIntegrationTest$PlainReporter"),
        }
    )
    public void withPlainReporterUuidIsNull() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG, true);
        try (Admin admin = clusterInstance.admin(configs)) {
            Uuid uuid = admin.clientInstanceId(Duration.ofSeconds(1));
            assertNull(uuid);
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "metric.reporters", value = "kafka.server.integration.DemoTelemetryIntegrationTest$ClientTelemetryAwareReporter"),
        }
    )
    public void withClientTelemetryReporterUuidIsCreated() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG, true);
        try (Admin admin = clusterInstance.admin(configs)) {
            Uuid uuid = admin.clientInstanceId(Duration.ofSeconds(1));
            assertNotNull(uuid);
            System.out.println(uuid);
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = "metric.reporters", value = "kafka.server.integration.DemoTelemetryIntegrationTest$ClientTelemetryAwareReporter"),
        }
    )
    public void withDefaultClientConfigExceptionIsThrown() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            try {
                admin.clientInstanceId(Duration.ofSeconds(1));
                fail("java.lang.IllegalStateException expected");
            } catch (IllegalStateException expected) {
                System.out.println(expected.getMessage());
            }
        }
    }

    public static class PlainReporter implements MetricsReporter {
        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    public static class ClientTelemetryAwareReporter extends PlainReporter implements ClientTelemetry {
        private final ClientTelemetryReceiver clientTelemetryReceiver = mock(ClientTelemetryReceiver.class);
        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return clientTelemetryReceiver;
        }
    }
}
