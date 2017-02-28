package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.server.HyperBrokerPlugin;
import scala.jdk.javaapi.OptionConverters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HyperBrokerPluginDynamicPropsTest {

    @ClusterTest(types = {Type.KRAFT},
            serverProperties = {
                @ClusterConfigProperty(key = HyperBrokerPlugin.PROP_NAME, value = "kafka.server.HyperBrokerPluginDynamicPropsTest$TestHyperPlugin"),
                @ClusterConfigProperty(key = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, value = "PLAIN,OAUTHBEARER"),
            })
    public void testGetsUpdatedConfigs(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            KafkaBroker kafka0 = cluster.brokers().get(0);
            TestHyperPlugin hyperplugin0 = (TestHyperPlugin) OptionConverters.toJava(kafka0.dataPlaneRequestProcessor().hyperplugin()).get();

            System.out.println("**** before **** " + hyperplugin0.configs.toString());
            System.out.println("**** before **** " + hyperplugin0.configRepository.brokerConfig(0).toString());

            ConfigResource broker0 = new ConfigResource(ConfigResource.Type.BROKER, "0");
            // NOTE the alterConfig can apparently contain any key, including custom properties, and they will be persisted ...
            ConfigEntry configEntry = new ConfigEntry("sasl.enabled.mechanisms", "OAUTHBEARER");
            AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
            admin.incrementalAlterConfigs(Map.of(broker0, List.of(alterConfigOp))).all().get();

            Thread.sleep(2000);
            System.out.println("**** after **** " + hyperplugin0.configs.toString());
            System.out.println("**** after **** " + hyperplugin0.configRepository.brokerConfig(0).toString());

            kafka0.shutdown();
            kafka0.awaitShutdown();
            kafka0.startup();
            cluster.waitForReadyBrokers();

            hyperplugin0 = (TestHyperPlugin) OptionConverters.toJava(kafka0.dataPlaneRequestProcessor().hyperplugin()).get();
            System.out.println("**** after restart **** " + hyperplugin0.configs.toString());
            System.out.println("**** after restart **** " + hyperplugin0.configRepository.brokerConfig(0).toString());
        }
    }

    public static class TestHyperPlugin implements HyperBrokerPlugin {

        Map<String, ?> configs;
        ConfigRepository configRepository;

        @Override
        public void setKafkaApis(Object kafkaApis) {
            this.configRepository = ((KafkaApis) kafkaApis).configRepository();
        }

        @Override
        public void close() throws IOException {
            configs =  null;
            configRepository =  null;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
        }
    }
}
