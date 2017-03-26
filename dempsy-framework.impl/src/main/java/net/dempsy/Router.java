package net.dempsy;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.utils.PersistentTask;

public class Router extends Dispatcher implements Service {
    private static Logger LOGGER = LoggerFactory.getLogger(Router.class);
    private static final long RETRY_TIMEOUT = 500L;

    ClusterInfoSession session;

    private final ConcurrentHashMap<ClusterId, RoutingStrategy> routStragByCid = new ConcurrentHashMap<>();
    private PersistentTask checkup;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public Router(final ClusterInfoSession session) {
        this.session = session;
    }

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
        checkup = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {
            private final ClusterInfoSession session = infra.getCollaborator();

            @Override
            public boolean execute() {

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
