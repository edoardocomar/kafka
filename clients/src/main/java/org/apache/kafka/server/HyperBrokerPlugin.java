package org.apache.kafka.server;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.Optional;

/**
 * HyperBrokerPlugin is an HyperPlugin defined plug point for broker servers.
 *
 * It is also used to propagate the KafkaApi object handle into other components.
 * It is instantiated via an entry in KafkaConfig named {@value PROP_NAME}
 */
public interface HyperBrokerPlugin extends Configurable, Closeable {
    // property name used in KafkaConfig
    String PROP_NAME = "hyper.broker.plugin.class.name";

    default <T extends AbstractRequest> T interceptRequest(
            KafkaPrincipal principal, RequestHeader requestHeader, T requestBody) {
        return requestBody;
    }

    default <T extends AbstractResponse> T interceptResponse(
            KafkaPrincipal principal, RequestHeader requestHeader, T responseBody) {
        return responseBody;
    }

    default InetAddress interceptClientAddress(KafkaPrincipal principal, InetAddress clientAddress) {
        return clientAddress;
    }

    /**
     * if the returned response is not empty, KafkaApis will not handle the request
     * and the response created by this plugin will be sent to the client instead
     */
    default <T extends AbstractRequest, S extends AbstractResponse> Optional<S> bypassApi(
            KafkaPrincipal principal, RequestHeader requestHeader, T requestBody) {
        return Optional.empty();
    }

    // The KafkaApis type is not visible in the client project, so we use an opaque reference.
    // A KafkaApis instance can be used by the implementation to get a handle to the metadata cache
    void setKafkaApis(Object kafkaApis);
}
