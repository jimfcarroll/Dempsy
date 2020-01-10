package net.dempsy.router.direct;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.utils.PersistentTask;

/**
 * This Strategy assumes the message key has the direct address of the
 * destination. It allows you to send messages directly to a particular
 * container (node/cluster combination) if you can figure out the guid
 * of the {@link DirectInboundSide} instance you want.
 */
public class DirectRoutingStrategy implements RoutingStrategy.Router {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectRoutingStrategy.class);
    private static final long RETRY_TIMEOUT = 500L;

    private String rootDir;

    private transient AtomicBoolean isRunning = new AtomicBoolean(false);
    private transient ClusterInfoSession session;
    private transient PersistentTask keepUpToDate;

    final ClusterId thisClusterId;
    private final DirectRoutingStrategyFactory factory;

    private final AtomicReference<Map<String, ContainerAddress>> address = new AtomicReference<>();
    private final AtomicBoolean isReady = new AtomicBoolean(false);

    DirectRoutingStrategy(final DirectRoutingStrategyFactory mom, final ClusterId clusterId, final Infrastructure infra) {
        this.factory = mom;
        this.thisClusterId = clusterId;
        this.session = infra.getCollaborator();
        this.rootDir = infra.getRootPaths().clustersDir + "/" + clusterId.clusterName;
        this.isRunning = new AtomicBoolean(false);

        this.keepUpToDate = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {

            @Override
            public boolean execute() {
                try {
                    final Collection<String> clusterDirs = session.getSubdirs(rootDir, this);

                    if(clusterDirs.size() == 0) {
                        LOGGER.debug("Checking on registered node for " + clusterId + " yields no registed nodes yet");
                        address.set(null);
                        return false;
                    } else {
                        final Map<String, ContainerAddress> addresses = new HashMap<>();
                        for(final String guid: clusterDirs) {
                            final ContainerAddress addr = (ContainerAddress)session.getData(rootDir + "/" + guid, null);
                            if(address == null) {
                                LOGGER.debug("ContainerAddress missing for " + clusterId + " for destination guid " + guid + ". Trying again.");
                                address.set(null);
                                return false;
                            }
                            addresses.put(guid, addr);
                        }
                        address.set(addresses);
                        isReady.set(true);
                        return true;
                    }

                } catch(final ClusterInfoException e) {
                    LOGGER.debug("Failed attempt to retreive node destination information:" + e.getLocalizedMessage());
                    return false;
                }
            }

            @Override
            public String toString() {
                return "find nodes using " + DirectRoutingStrategy.class.getSimpleName() + " for cluster " + clusterId;
            }
        };

        isRunning.set(true);
        keepUpToDate.process();
    }

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        if(!isRunning.get())
            throw new IllegalStateException(
                "attempt to use " + DirectRoutingStrategy.class.getSimpleName() + " prior to starting it or after stopping it.");

        final Object messageKey = message.key;
        if(Key.class.isAssignableFrom(messageKey.getClass())) {
            final Key key = (Key)messageKey;
            final Map<String, ContainerAddress> whereToSend = address.get();
            return whereToSend != null ? whereToSend.get(key.destinationGuid) : null;
        } else
            throw new DempsyException(
                "The " + DirectRoutingStrategy.class.getSimpleName() + " can only be used with message keys of type " + Key.class.getName());
    }

    @Override
    public Collection<ContainerAddress> allDesintations() {
        return Optional.ofNullable(address.get())
            .map(m -> m.values())
            .orElse(Collections.emptyList());
    }

    @Override
    public void release() {
        factory.release(this);
        isRunning.set(false);
    }

    boolean isReady() {
        return isReady.get();
    }
}
