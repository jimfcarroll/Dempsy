package net.dempsy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Outbound;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.TransportManager;
import net.dempsy.util.SafeString;
import net.dempsy.utils.PersistentTask;

public class Router extends Dispatcher implements Service {
    private static Logger LOGGER = LoggerFactory.getLogger(Router.class);
    private static final long RETRY_TIMEOUT = 500L;

    private PersistentTask checkup;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final RoutingStrategyManager manager;
    private final AtomicReference<ApplicationState> outbounds = new AtomicReference<>(null);

    private final NodeAddress thisNode;
    private final TransportManager tmanager;
    private final NodeReceiver nodeReciever;

    private static class ApplicationState {
        public final Map<String, Outbound> outboundByClusterName = new HashMap<>();
        public final Map<String, List<String>> clusterNameByMessageType = new HashMap<>();
        public final Map<String, SenderFactory> senderByTypeId = new HashMap<>();
        public final Map<NodeAddress, SenderFactory> senderByNode = new HashMap<>();
    }

    public Router(final RoutingStrategyManager manager, final NodeAddress thisNode, final NodeReceiver nodeReciever,
            final TransportManager tmanager) {
        this.manager = manager;
        this.thisNode = thisNode;
        this.tmanager = tmanager;
        this.nodeReciever = nodeReciever;
    }

    @Override
    public void dispatch(final KeyedMessageWithType message) {
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("Dispatching " + SafeString.objectDescription(message.message) + ".");

        final ApplicationState cur = outbounds.get();
        if (cur == null) {
            // we're down, or something.
            return;
        }
        final Map<String, List<String>> clusterNameByMessageType = cur.clusterNameByMessageType;
        final Map<String, Outbound> outboundByClusterName = cur.outboundByClusterName;
        final Set<String> uniqueClusters = new HashSet<>();

        Arrays.stream(message.messageTypes).forEach(mt -> {
            final List<String> clusterNames = clusterNameByMessageType.get(mt);
            if (clusterNames == null)
                LOGGER.info("No cluster that handles messages of type " + mt);
            else
                uniqueClusters.addAll(clusterNames);
        });

        final Map<NodeAddress, ContainerAddress> containerByNodeAddress = new HashMap<>();
        uniqueClusters.forEach(cn -> {
            final Outbound ob = outboundByClusterName.get(cn);
            final ContainerAddress ca = ob.selectDestinationForMessage(message);
            // it's possible ca is null
            if (ca == null)
                LOGGER.info("No way to send the message {} to the cluster {} for the time being", message, cn);
            else {
                final ContainerAddress already = containerByNodeAddress.get(ca.node);
                if (already != null) {
                    final int[] ia = new int[already.clusters.length + ca.clusters.length];
                    System.arraycopy(already.clusters, 0, ia, 0, already.clusters.length);
                    System.arraycopy(ca.clusters, 0, ia, already.clusters.length, ca.clusters.length);
                    containerByNodeAddress.put(ca.node, new ContainerAddress(ca.node, ia));
                } else
                    containerByNodeAddress.put(ca.node, ca);
            }
        });

        containerByNodeAddress.entrySet().stream().forEach(e -> {
            final NodeAddress curNode = e.getKey();
            final ContainerAddress curAddr = e.getValue();

            final RoutedMessage toSend = new RoutedMessage(curAddr.clusters, message.key, message.message);
            if (thisNode.equals(curNode))
                nodeReciever.feedbackLoop(toSend);
            else {
                final SenderFactory sf = cur.senderByNode.get(curNode);
                if (sf == null)
                    LOGGER.error("Couldn't send message to " + curNode + " because there's no " + SenderFactory.class.getSimpleName());
                else
                    // TODO: more error handling
                    sf.getSender(curNode).send(toSend);
            }
        });
    }

    @Override
    public void start(final Infrastructure infra) {
        final ClusterInfoSession session = infra.getCollaborator();
        final String nodesDir = infra.getRootPaths().nodesDir;

        checkup = new PersistentTask(LOGGER, isRunning, infra.getScheduler(), RETRY_TIMEOUT) {

            @Override
            public boolean execute() {
                // stop all already running strategies.
                final ApplicationState obs = outbounds.getAndSet(null);
                stopEm(obs);

                try {
                    final Collection<String> nodeDirs = session.getSubdirs(nodesDir, this);

                    final ApplicationState newOutbounds = new ApplicationState();

                    final Set<NodeAddress> alreadySeen = new HashSet<>();
                    // get all of the subdirectories NodeInformations
                    for (final String subdir : nodeDirs) {
                        final NodeInformation ni = (NodeInformation) session.getData(nodesDir + "/" + subdir, null);

                        if (ni == null) {
                            LOGGER.warn("A node directory was empty at " + subdir);
                            return false;
                        }

                        // see if node info is dupped.
                        if (alreadySeen.contains(ni.nodeAddress)) {
                            LOGGER.warn("The node " + ni.nodeAddress + " seems to be registed more than once.");
                            continue;
                        }
                        alreadySeen.add(ni.nodeAddress);

                        try {
                            SenderFactory sf = newOutbounds.senderByTypeId.get(ni.transportTypeId);
                            if (sf == null) {
                                sf = tmanager.getAssociatedInstance(ni.transportTypeId);
                                newOutbounds.senderByTypeId.put(ni.transportTypeId, sf);
                            }
                            newOutbounds.senderByNode.put(ni.nodeAddress, sf);
                        } catch (final DempsyException de) {
                            LOGGER.warn("Couldn't create a transport to the node " + ni);
                            continue;
                        }

                        final Map<ClusterId, ClusterInformation> known = new HashMap<>();

                        final Collection<ClusterInformation> cis = ni.clusterInfoByClusterId.values();
                        for (final ClusterInformation ci : cis) {
                            // do we know about it already.
                            final ClusterInformation existing = known.get(ci.clusterId);
                            if (existing != null) {
                                // check to make sure it's the same.
                                if (!existing.equals(ci)) {
                                    LOGGER.warn("The cluster " + ci.clusterId + " is in multiple nodes with different configurations. In the node "
                                            + ni.nodeAddress.getGuid() + " it appears as " + ci + " but it was previously found as " + existing
                                            + ". Continuing assuming the later is correct.");
                                }

                                continue;
                            }

                            known.put(ci.clusterId, ci);

                            final RoutingStrategy.Factory obfactory = manager.getAssociatedInstance(ci.routingStrategyTypeId);
                            final Outbound ob = obfactory.getStrategy(ci.clusterId);

                            final String clusterName = ci.clusterId.clusterName;

                            newOutbounds.outboundByClusterName.put(clusterName, ob);
                            // for each messageType add an entry
                            ci.messageTypesHandled.forEach(mt -> {
                                List<String> cur = newOutbounds.clusterNameByMessageType.get(mt);
                                if (cur == null) {
                                    cur = new ArrayList<>();
                                    newOutbounds.clusterNameByMessageType.put(mt, cur);
                                }
                                cur.add(clusterName);
                            });

                            ob.start(infra);
                        }

                        outbounds.set(newOutbounds);
                    }
                    return true;
                } catch (final ClusterInfoException e) {
                    final String message = "Failed to find outgoing route information. Will retry shortly.";
                    if (LOGGER.isTraceEnabled())
                        LOGGER.debug(message, e);
                    else LOGGER.debug(message);
                    return false;
                }
            }

            @Override
            public String toString() {
                return "find nodes to route to";
            }
        };

        isRunning.set(true);
        checkup.process();
    }

    @Override
    public void stop() {
        isRunning.set(false);
        stopEm(outbounds.getAndSet(null));
    }

    /**
     * Strictly for testing.
     */
    public boolean canReach(final String cluterName, final KeyedMessageWithType message) {
        final ApplicationState cur = outbounds.get();
        if (cur == null)
            return false;
        final Outbound ob = cur.outboundByClusterName.get(cluterName);
        if (ob == null)
            return false;
        final ContainerAddress ca = ob.selectDestinationForMessage(message);
        if (ca == null)
            return false;
        return true;
    }

    public static class RoutedMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        public final int[] containers;
        public final Object key;
        public final Object message;

        public RoutedMessage(final int[] containers, final Object key, final Object message) {
            this.containers = containers;
            this.key = key;
            this.message = message;
        }
    }

    private static void stopEm(final ApplicationState obs) {}
}
