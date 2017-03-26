package net.dempsy.router.simple;

import static net.dempsy.util.Functional.recheck;
import static net.dempsy.util.Functional.uncheck;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.utils.PersistentTask;

/**
 * This simple strategy expects at most a single node to implement any given message.
 */
public class SimpleRoutingStrategy implements RoutingStrategy.Outbound {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRoutingStrategy.class);
    private static final long timeoutMillis = 500L;

    private String rootDir;

    private transient AtomicBoolean isRunning = new AtomicBoolean(false);
    private transient ClusterInfoSession session;
    private transient PersistentTask keepUpToDate;

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        if (isRunning.get())
            throw new IllegalStateException(
                    "attempt to use " + SimpleRoutingStrategy.class.getSimpleName() + " prior to starting it or after stopping it.");

        return null;
    }

    @Override
    public void start(final Infrastructure infra) {
        this.session = infra.getCollaborator();
        this.rootDir = infra.getRootPaths().clustersDir;
        this.isRunning = new AtomicBoolean(false);

        this.keepUpToDate = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), timeoutMillis) {

            @Override
            public boolean execute() {
                try {
                    final Collection<String> clusterDirs = session.getSubdirs(rootDir, this);

                    recheck(() -> clusterDirs.forEach(cd -> uncheck(() -> {
                        session.getData(rootDir + "/" + cd, null);
                    })), ClusterInfoException.class);

                } catch (final ClusterInfoException e) {
                    LOGGER.debug("Failed attempt to retreive cluster information");
                    return false;
                }

                return true;
            }
        };

        isRunning.set(true);
        keepUpToDate.process();
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }

}
