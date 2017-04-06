package net.dempsy;

import java.util.Map;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public interface Infrastructure {
    ClusterInfoSession getCollaborator();

    AutoDisposeSingleThreadScheduler getScheduler();

    RootPaths getRootPaths();

    ClusterStatsCollector getClusterStatsCollector(ClusterId clusterId);

    NodeStatsCollector getNodeStatsCollector();

    Map<String, String> getConfiguration();

    public default String getConfigValue(final Class<?> clazz, final String key, final String defaultValue) {
        final Map<String, String> conf = getConfiguration();
        final String entireKey = clazz.getPackage().getName() + "." + key;
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    public static class RootPaths {
        public final String rootDir;
        public final String nodesDir;
        public final String clustersDir;

        public RootPaths(final String rootDir, final String nodesDir, final String clustersDir) {
            this.rootDir = rootDir;
            this.nodesDir = nodesDir;
            this.clustersDir = clustersDir;
        }
    }
}
