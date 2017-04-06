package net.dempsy.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.SafeString;

/**
 * This class is a builder for the
 */
public class Node {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);
    private static final String DEFAULT_APP = "default";

    public final String application;
    private final List<Cluster> clusters = new ArrayList<>();
    private String clusterStatsCollectorFactoryId = "net.dempsy.monitoring.dummy";
    private Object nodeStatsCollector = null;
    private String defaultRoutingStrategyId = null;
    private Object receiver = null;
    private boolean configed = false;
    private String containerTypeId = "net.dempsy.container.nonlocking";
    private final Map<String, String> configuration = new HashMap<>();

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

    public Node clusterStatsCollectorFactoryId(final String statsCollector) {
        this.clusterStatsCollectorFactoryId = statsCollector;
        return this;
    }

    public Node nodeStatsCollector(final Object statsCollector) {
        this.nodeStatsCollector = statsCollector;
        return this;
    }

    public Node defaultRoutingStrategyId(final String rs) {
        this.defaultRoutingStrategyId = rs;
        return this;
    }

    public Node conf(final String key, final String value) {
        String oldVal;
        if ((oldVal = configuration.putIfAbsent(key, value)) != null) {
            LOGGER.warn("Configuration value for \"" + key + "\" was already set to \"" + oldVal + "\" but is being changed to \"" + value + "\"");
            configuration.put(key, value);
        }
        return this;
    }

    public Node configure() {
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

    public Node setClusterStatsCollectorFactoryId(final String statsCollector) {
        return clusterStatsCollectorFactoryId(statsCollector);
    }

    public String getClusterStatsCollectorFactoryId() {
        return clusterStatsCollectorFactoryId;
    }

    public Node setNodeStatsCollector(final Object statsCollector) {
        return nodeStatsCollector(statsCollector);
    }

    public Object getNodeStatsCollector() {
        return nodeStatsCollector;
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

    public Node setConfiguration(final Map<String, String> conf) {
        configuration.clear();
        configuration.putAll(conf);
        return this;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void validate() throws IllegalStateException {
        configure();

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
