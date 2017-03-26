package net.dempsy;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public interface Infrastructure {
    ClusterInfoSession getCollaborator();

    AutoDisposeSingleThreadScheduler getScheduler();

    RootPaths getRootPaths();

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
