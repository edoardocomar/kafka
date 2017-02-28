package org.apache.kafka.server;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;

import java.io.Closeable;

/**
 * HyperControllerPlugin is an HyperPlugin defined plug point for the controller server.
 *
 * It is also used to propagate the ControllerApis object handle into other components.
 *
 * It is instantiated via an entry in KafkaConfig named {@value PROP_NAME}
 */
public interface HyperControllerPlugin extends Configurable, Closeable {
    // property names used in KafkaConfig
    String PROP_NAME = "hyper.controller.plugin.class.name";

    // The ControllerApis type is not visible in the client project, so we use an opaque reference.
    // A ControllerApis instance can be used by the implementation to get a handle to the metadata cache
    void setControllerApis(Object controllerApis, Metrics kafkaMetrics);
}
