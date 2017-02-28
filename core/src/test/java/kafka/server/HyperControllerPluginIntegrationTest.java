package kafka.server;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.server.HyperControllerPlugin;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.util.Map;

import static org.apache.kafka.server.HyperControllerPlugin.PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HyperControllerPluginIntegrationTest {
    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = PROP_NAME, value = "kafka.server.HyperControllerPluginIntegrationTest$TestControllerPlugin"),
    })
    public void testControllerPluginIntegration(ClusterInstance cluster) throws Exception {
        cluster.waitForReadyBrokers();

        ControllerServer controllerServer = cluster.controllers().values().iterator().next();
        assertTrue(controllerServer.eventStreamsPlugin().nonEmpty());

        TestControllerPlugin pluginInstance = (TestControllerPlugin) controllerServer.eventStreamsPlugin().get();

        assertNotNull(pluginInstance.configs);
        assertNotNull(pluginInstance.controllerApis);
        assertNotNull(pluginInstance.metrics);
        assertEquals(0, pluginInstance.closeCalled);

        cluster.stop();

        TestUtils.waitForCondition(() -> cluster.stopped(), "cluster did not stop");

        assertEquals(1, pluginInstance.closeCalled);
    }

    public static class TestControllerPlugin implements HyperControllerPlugin {

        Map<String, ?> configs;
        ControllerApis controllerApis;
        Metrics metrics;
        int closeCalled = 0;

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
        }

        @Override
        public void setControllerApis(Object controllerApis, Metrics kafkaMetrics) {
            this.controllerApis = (ControllerApis) controllerApis;
            this.metrics = kafkaMetrics;
        }

        @Override
        public void close() throws IOException {
            closeCalled++;
        }
    }
}
