package org.apache.flink.connector.pulsar.testutils.runtime.remote;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import static org.testcontainers.containers.PulsarContainer.BROKER_PORT;

/** The runtime which will connect to a remote instance. It's always used for local debugging. */
public class PulsarRemoteRuntime implements PulsarRuntime {

    private final String serviceUrl;
    private final String adminUrl;

    private PulsarRuntimeOperator operator;

    public PulsarRemoteRuntime(String host) {
        this("pulsar://" + host + ":" + BROKER_PORT, "http://" + host + ":" + BROKER_HTTP_PORT);
    }

    public PulsarRemoteRuntime(String serviceUrl, String adminUrl) {
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
    }

    @Override
    public PulsarRuntime withConfigs(Map<String, String> configs) {
        if (!configs.isEmpty()) {
            throw new UnsupportedOperationException(
                    "We can't change the broker configs on a running instance.");
        }

        return this;
    }

    @Override
    public void startUp() throws Exception {
        this.operator = new PulsarRuntimeOperator(serviceUrl, adminUrl);
    }

    @Override
    public void tearDown() {
        // Nothing to do here.
    }

    @Override
    public PulsarRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this pulsar runtime first.");
    }
}
