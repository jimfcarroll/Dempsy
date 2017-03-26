package net.dempsy;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.utils.PersistentTask;

public class Router extends Dispatcher implements Service {
    private static Logger LOGGER = LoggerFactory.getLogger(Router.class);
    private static final long RETRY_TIMEOUT = 500L;

    private ClusterInfoSession session;

    private final ConcurrentHashMap<ClusterId, RoutingStrategy.Outbound> routStragByCid = new ConcurrentHashMap<>();
    private PersistentTask checkup;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @Override
    public void dispatch(final KeyedMessageWithType message) {

    }

    public static class RoutedMessage implements Serializable {
        private static final long serialVersionUID = 1L;

        public final int container;
        public final Object key;
        public final Object message;

        public RoutedMessage(final int container, final Object key, final Object message) {
            this.container = container;
            this.key = key;
            this.message = message;
        }
    }

    @Override
    public void start(final Infrastructure infra) {
        session = infra.getCollaborator();
        final String clustersDir = infra.getRootPaths().clustersDir;

        checkup = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {

            @Override
            public boolean execute() {

                try {
                    final Collection<String> clusters = session.getSubdirs(clustersDir, this);

                } catch (final ClusterInfoException e) {
                    final String message = "Failed to find outgoing route information. Will retry shortly.";
                    if (LOGGER.isTraceEnabled())
                        LOGGER.debug(message, e);
                    else LOGGER.debug(message);
                    return false;
                }
                return false;
            }
        };

        isRunning.set(true);
        checkup.process();
    }

    @Override
    public void stop() {

    }
}
