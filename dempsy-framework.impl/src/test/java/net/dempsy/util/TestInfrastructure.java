package net.dempsy.util;

import java.util.HashMap;
import java.util.Map;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.monitoring.basic.BasicNodeStatsCollector;
import net.dempsy.monitoring.basic.BasicStatsCollectorFactory;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public class TestInfrastructure implements Infrastructure {
    final ClusterInfoSession session;
    final AutoDisposeSingleThreadScheduler sched;
    final BasicStatsCollectorFactory statsFact;
    final BasicNodeStatsCollector nodeStats;

    public TestInfrastructure(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        this.session = session;
        this.sched = sched;
        statsFact = new BasicStatsCollectorFactory();
        nodeStats = new BasicNodeStatsCollector();
    }

    @Override
    public ClusterInfoSession getCollaborator() {
        return session;
    }

    @Override
    public AutoDisposeSingleThreadScheduler getScheduler() {
        return sched;
    }

    @Override
    public RootPaths getRootPaths() {
        return new RootPaths("/application", "/application/nodes", "/application/clusters");
    }

    @Override
    public ClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
        return statsFact.createStatsCollector(clusterId, null);
    }

    @Override
    public Map<String, String> getConfiguration() {
        return new HashMap<>();
    }

    @Override
    public NodeStatsCollector getNodeStatsCollector() {
        return nodeStats;
    }
}