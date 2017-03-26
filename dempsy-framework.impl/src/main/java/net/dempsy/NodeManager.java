package net.dempsy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.container.Container;
import net.dempsy.messages.Adaptor;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.util.SafeString;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;
import net.dempsy.utils.PersistentTask;

public class NodeManager implements Infrastructure {
    private final static Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);
    private static final long RETRY_PERIOND_MILLIS = 500L;

    private Node node = null;
    private ClusterInfoSession session;
    private final List<PerContainer> containers = new ArrayList<>();
    private final List<Adaptor> adaptors = new ArrayList<>();
    private Router router = null;
    private final AutoDisposeSingleThreadScheduler persistenceScheduler = new AutoDisposeSingleThreadScheduler("Dempsy-pestering-");

    private PersistentTask keepNodeRegstered = null;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private RootPaths rootPaths = null;

    public NodeManager node(final Node node) {
        this.node = node;
        final String appName = node.application;
        this.rootPaths = new RootPaths(root(appName), nodes(appName), clusters(appName));
        return this;
    }

    public NodeManager collaborator(final ClusterInfoSession session) {
        if (session == null)
            throw new NullPointerException("Cannot pass a null collaborator to " + NodeManager.class.getSimpleName());
        if (this.session != null)
            throw new IllegalStateException("Collaborator session is already set on " + NodeManager.class.getSimpleName());
        this.session = session;
        this.router = new Router(session);
        return this;
    }

    public NodeManager start() throws DempsyException {
        validate();

        // =====================================
        // set the dispatcher on adaptors and create containers for mp clusters
        node.getClusters().forEach(c -> {
            if (c.isAdaptor()) {
                final Adaptor adaptor = c.getAdaptor();
                adaptor.setDispatcher(router);
                adaptors.add(adaptor);
            } else {
                final Container con = new Container(c.getMessageProcessor(), c.getClusterId());
                con.setDispatcher(router);

                // TODO: This is a hack for now.
                final Manager<RoutingStrategy.Inbound> inboundManager = new Manager<RoutingStrategy.Inbound>(RoutingStrategy.Inbound.class);
                final RoutingStrategy.Inbound is = inboundManager.getAssociatedSenderFactory(c.getRoutingStrategyId());
                containers.add(new PerContainer(con, is));
            }
        });
        // =====================================

        // =====================================
        // register node with session
        // =====================================
        // first gather node information
        final Receiver receiver = (Receiver) node.getReceiver();
        final NodeAddress nodeAddress = receiver.getAddress();
        final String nodeId = nodeAddress.getGuid();
        final Map<ClusterId, Set<String>> messageTypesByClusterId = new HashMap<>();
        node.getClusters().forEach(c -> messageTypesByClusterId.put(c.getClusterId(), c.getMessageProcessor().messagesTypesHandled()));
        final NodeInformation nodeInfo = new NodeInformation(receiver.transportTypeId(), nodeAddress, messageTypesByClusterId);

        // Then actually register
        keepNodeRegstered = new PersistentTask(LOGGER, isRunning, persistenceScheduler, RETRY_PERIOND_MILLIS) {
            @Override
            public boolean execute() {
                try {
                    final String application = node.application;
                    makeRootDirsForApplication(session, application);

                    final String nodePath = nodes(application) + "/" + nodeId;

                    session.mkdir(nodePath, nodeInfo, DirMode.EPHEMERAL);
                    final NodeInformation reread = (NodeInformation) session.getData(nodePath, this);
                    return nodeInfo.equals(reread);
                } catch (final ClusterInfoException e) {
                    final String logmessage = "Failed to register the node. Retrying in " + RETRY_PERIOND_MILLIS + " milliseconds.";
                    if (LOGGER.isDebugEnabled())
                        LOGGER.info(logmessage, e);
                    else
                        LOGGER.info(logmessage);
                }
                return false;
            }

            @Override
            public String toString() {
                return "register node information";
            }
        };

        isRunning.set(true);
        keepNodeRegstered.process();
        // =====================================

        return this;
    }

    public List<Container> getContainers() {
        return Collections.unmodifiableList(containers.stream().map(pc -> pc.container).collect(Collectors.toList()));
    }

    public NodeManager validate() throws DempsyException {
        if (node == null)
            throw new DempsyException("No node set");

        node.validate();

        if (!Receiver.class.isAssignableFrom(node.getReceiver().getClass()))
            throw new DempsyException("The Node doesn't contain a " + Receiver.class.getSimpleName() + ". Instead it has a "
                    + SafeString.valueOfClass(node.getReceiver()));

        // if session is non-null, then so is the Router.
        if (session == null)
            throw new DempsyException("There's no collaborator set for this \"" + NodeManager.class.getSimpleName() + "\" ");

        return this;
    }

    @Override
    public ClusterInfoSession getCollaborator() {
        return session;
    }

    @Override
    public AutoDisposeSingleThreadScheduler getScheduler() {
        return persistenceScheduler;
    }

    @Override
    public RootPaths getRootPaths() {
        return rootPaths;
    }

    private static class PerContainer {
        final Container container;
        final RoutingStrategy.Inbound inboundStrategy;

        public PerContainer(final Container container, final Inbound inboundStrategy) {
            this.container = container;
            this.inboundStrategy = inboundStrategy;
        }
    }

    private static String root(final String application) {
        return "/" + application;
    }

    private static String nodes(final String application) {
        return root(application) + "/nodes";
    }

    private static String clusters(final String application) {
        return root(application) + "/clusters";
    }

    private static String cluster(final ClusterId clusterId) {
        return clusters(clusterId.applicationName) + "/" + clusterId.clusterName;
    }

    private static void makeRootDirsForApplication(final ClusterInfoSession session, final String application) throws ClusterInfoException {
        session.mkdir(root(application), null, DirMode.PERSISTENT);
        session.mkdir(nodes(application), null, DirMode.PERSISTENT);
        session.mkdir(clusters(application), null, DirMode.PERSISTENT);
    }

}
