package net.dempsy.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.dempsy.util.SafeString;

/**
 * This class is a builder for the
 */
public class Node {
    private static final String DEFAULT_APP = "default";

    public final String application;
    private final List<Cluster> clusters = new ArrayList<>();
    private Object statsCollector = null;
    private String defaultRoutingStrategyId = null;
    private Object receiver = null;
    private boolean configed = false;
    private String containerTypeId = "net.dempsy.container.nonlocking";

    public Node(final String applicationName) {
        if (applicationName == null)
            throw new IllegalArgumentException("You must set the application name while configuring a Dempsy application.");

        if (applicationName.contains("/") || applicationName.contains("\\"))
            throw new IllegalArgumentException("The application name must not contain slashes. The one provided is \"" + applicationName + "\"");

        this.application = applicationName;
    }

    public Node() {
        this(DEFAULT_APP);
    }

    public Cluster cluster(final String clusterName) {
        final Cluster ret = new Cluster(application, clusterName);
        clusters.add(ret);
        return ret;
    }

    public Node statsCollector(final Object statsCollector) {
        this.statsCollector = statsCollector;
        return this;
    }

    public Node defaultRoutingStrategyId(final String rs) {
        this.defaultRoutingStrategyId = rs;
        return this;
    }

    public Node config() {
        if (!configed) {
            clusters.forEach(c -> fillout(c));
            configed = true;
        }
        return this;
    }

    public Node receiver(final Object receiver) {
        this.receiver = receiver;
        return this;
    }

    public Node containerTypeId(final String containerTypeId) {
        this.containerTypeId = containerTypeId;
        return this;
    }

    // =======================================================================

    public Node setClusters(final Cluster... defs) {
        return setClusters(Arrays.asList(defs));
    }

    public Node setClusters(final List<Cluster> defs) {
        if (defs == null)
            throw new IllegalArgumentException("Cannot pass a null set of " + Cluster.class.getSimpleName() + "s.");
        if (defs.size() == 0)
            throw new IllegalArgumentException("Cannot pass an empty set of " + Cluster.class.getSimpleName() + "s.");
        defs.forEach(c -> {
            if (c == null)
                throw new IllegalArgumentException("Cannot pass a null " + Cluster.class.getSimpleName() + ".");
        });
        clusters.addAll(defs);
        return this;
    }

    public List<Cluster> getClusters() {
        return Collections.unmodifiableList(clusters);
    }

    /**
     * Get the {@link Cluster} that corresponds to the given clusterId.
     */
    public Cluster getCluster(final ClusterId clusterId) {
        for (final Cluster cur : clusters)
            if (cur.getClusterId().equals(clusterId))
                return cur;
        return null;
    }

    /**
     * Get the {@link Cluster} that corresponds to the given clusterId.
     */
    public Cluster getCluster(final String clusterId) {
        return getCluster(new ClusterId(application, clusterId));
    }

    public Node setDefaultRoutingStrategyId(final String rs) {
        return defaultRoutingStrategyId(rs);
    }

    public Object getDefaultRoutingStrategyId() {
        return defaultRoutingStrategyId;
    }

    public Node setStatsCollector(final Object statsCollector) {
        return statsCollector(statsCollector);
    }

    public Object getStatsCollector() {
        return statsCollector;
    }

    public Node setReceiver(final Object receiver) {
        this.receiver = receiver;
        return this;
    }

    public Object getReceiver() {
        return receiver;
    }

    public Node setContainerTypeId(final String containerTypeId) {
        return containerTypeId(containerTypeId);
    }

    public String getContainerTypeId() {
        return containerTypeId;
    }

    public void validate() throws IllegalStateException {
        config();

        if (application == null)
            throw new IllegalStateException("You must set the application name while configuring a Dempsy application.");

        if (clusters.size() == 0)
            throw new IllegalStateException("The application \"" + SafeString.valueOf(application) + "\" doesn't have any clusters defined.");

        final Set<ClusterId> clusterNames = new HashSet<ClusterId>();

        for (final Cluster clusterDef : clusters) {
            if (clusterDef == null)
                throw new IllegalStateException("The application definition for \"" + application + "\" has a null ClusterDefinition.");

            if (clusterNames.contains(clusterDef.getClusterId()))
                throw new IllegalStateException(
                        "The application definition for \"" + application + "\" has two cluster definitions with the cluster id \""
                                + clusterDef.getClusterId() + "\"");

            clusterNames.add(clusterDef.getClusterId());

            clusterDef.validate();
        }

        if (receiver == null)
            throw new IllegalStateException("Cannot have a null reciever on a " + Node.class.getSimpleName());
    }

    private void fillout(final Cluster cd) {
        cd.setAppName(application);
        if (defaultRoutingStrategyId != null && cd.getRoutingStrategyId() == null)
            cd.setRoutingStrategyId(defaultRoutingStrategyId);
    }
}
