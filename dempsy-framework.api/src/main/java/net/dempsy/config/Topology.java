package net.dempsy.config;

import net.dempsy.lifecycle.annotations.MessageProcessor;
import net.dempsy.messages.Adaptor;

/**
 * This class is a builder for the
 * 
 * @author jim
 *
 */
public class Topology {
    final private ApplicationDefinition app;

    public Topology(final String applicationName) {
        app = new ApplicationDefinition(applicationName);
    }

    public Topology add(final String clusterName, final MessageProcessor mp) {
        app.add(new ClusterDefinition(clusterName, mp));
        return this;
    }

    public Topology add(final String clusterName, final Adaptor adaptor) {
        app.add(new ClusterDefinition(clusterName, adaptor));
        return this;
    }

    public ApplicationDefinition app() {
        app.validate();
        return app;
    }

    public Topology serializer(final Object serializer) {
        app.setSerializer(serializer);
        return this;
    }

    public Topology routing(final Object routingStrategy) {
        app.setRoutingStrategy(routingStrategy);
        return this;
    }

    public Topology statsCollector(final Object statsCollector) {
        app.setStatsCollectorFactory(statsCollector);
        return this;
    }

    public Topology add(final ClusterDefinition cd) {
        app.add(cd);
        return this;
    }
}
