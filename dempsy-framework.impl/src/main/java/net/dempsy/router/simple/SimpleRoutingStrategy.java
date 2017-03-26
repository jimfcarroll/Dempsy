package net.dempsy.router.simple;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.utils.PersistentTask;

/**
 * This simple strategy expects at most a single node to implement any given message.
 */
public class SimpleRoutingStrategy implements RoutingStrategy.Outbound {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRoutingStrategy.class);
    private static final long RETRY_TIMEOUT = 500L;

    private String rootDir;

    private transient AtomicBoolean isRunning = new AtomicBoolean(false);
    private transient ClusterInfoSession session;
    private transient PersistentTask keepUpToDate;

    private ClusterId clusterId;
    private final AtomicReference<ContainerAddress> address = new AtomicReference<>();

    @Override
    public void setClusterId(final ClusterId clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        if (!isRunning.get())
            throw new IllegalStateException(
                    "attempt to use " + SimpleRoutingStrategy.class.getSimpleName() + " prior to starting it or after stopping it.");

        return address.get();
    }

    @Override
    public void start(final Infrastructure infra) {
        this.session = infra.getCollaborator();
        this.rootDir = infra.getRootPaths().clustersDir + "/" + clusterId.clusterName;
        this.isRunning = new AtomicBoolean(false);

        this.keepUpToDate = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {

            @Override
            public boolean execute() {
                try {
                    final Collection<String> clusterDirs = session.getSubdirs(rootDir, this);

                    if (clusterDirs.size() > 1)
                        LOGGER.warn("There's more than one node registered for " + clusterId + " but it has a "
                                + SimpleRoutingStrategy.class.getSimpleName());

                    if (clusterDirs.size() == 0) {
                        LOGGER.debug("Checking on registered node for " + clusterId + " yields no registed nodes yet");
                        address.set(null);
                        return false;
                    } else {
                        final String nodeToSendTo = clusterDirs.iterator().next();

                        final ContainerAddress addr = (ContainerAddress) session.getData(rootDir + "/" + nodeToSendTo, null);
                        if (address == null) {
                            LOGGER.debug("ContainerAddress missing for " + clusterId + " at " + nodeToSendTo + ". Trying again.");
                            address.set(null);
                            return false;
                        }

                        address.set(addr);

                        return true;
                    }

                } catch (final ClusterInfoException e) {
                    LOGGER.debug("Failed attempt to retreive node destination information");
                    return false;
                }
            }

            @Override
            public String toString() {
                return "find nodes using " + SimpleRoutingStrategy.class.getSimpleName() + " for cluster " + clusterId;
            }

        };

        isRunning.set(true);
        keepUpToDate.process();
    }

    @Override
    public void stop() {
        isRunning.set(false);
    }

}
