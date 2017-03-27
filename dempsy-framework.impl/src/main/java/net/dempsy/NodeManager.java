package net.dempsy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DirMode;
import net.dempsy.config.Cluster;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.container.Container;
import net.dempsy.messages.Adaptor;
import net.dempsy.monitoring.DummyStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategy.Inbound;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;
import net.dempsy.transport.TransportManager;
import net.dempsy.util.SafeString;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;
import net.dempsy.utils.PersistentTask;

public class NodeManager implements Infrastructure, AutoCloseable {
    private final static Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);
    private static final long RETRY_PERIOND_MILLIS = 500L;

    private Node node = null;
    private ClusterInfoSession session;
    private final AutoDisposeSingleThreadScheduler persistenceScheduler = new AutoDisposeSingleThreadScheduler("Dempsy-pestering-");

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final ThreadingModel threading = new DefaultThreadingModel().setCoresFactor(1.0).setAdditionalThreads(1)
            .setMaxNumberOfQueuedLimitedTasks(10000).start();

    // created in start(). Stopped in stop()
    private Receiver receiver = null;
    private final List<PerContainer> containers = new ArrayList<>();
    private final List<Adaptor> adaptors = new ArrayList<>();
    private Router router = null;
    private PersistentTask keepNodeRegstered = null;
    private RootPaths rootPaths = null;
    private StatsCollector statsCollector;
    private RoutingStrategyManager rsManager = null;
    private TransportManager tManager = null;

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
        return this;
    }

    public NodeManager start() throws DempsyException {
        validate();

        statsCollector = (StatsCollector) node.getStatsCollector();
        if (statsCollector == null)
            statsCollector = new DummyStatsCollector();

        // =====================================
        // set the dispatcher on adaptors and create containers for mp clusters
        node.getClusters().forEach(c -> {
            if (c.isAdaptor()) {
                final Adaptor adaptor = c.getAdaptor();
                adaptors.add(adaptor);
            } else {
                final Container con = new Container(c.getMessageProcessor(), c.getClusterId());
                con.setStatCollector(statsCollector);

                // TODO: This is a hack for now.
                final Manager<RoutingStrategy.Inbound> inboundManager = new Manager<RoutingStrategy.Inbound>(RoutingStrategy.Inbound.class);
                final RoutingStrategy.Inbound is = inboundManager.getAssociatedInstance(c.getRoutingStrategyId());
                containers.add(new PerContainer(con, is, c));
            }
        });
        // =====================================

        // =====================================
        // register node with session
        // =====================================
        // first gather node information
        receiver = (Receiver) node.getReceiver();
        final NodeAddress nodeAddress = receiver.getAddress();

        final NodeReceiver nodeReciever = new NodeReceiver(containers.stream().map(pc -> pc.container).collect(Collectors.toList()), threading,
                statsCollector);
        receiver.start(nodeReciever, threading);

        final String nodeId = nodeAddress.getGuid();
        final Map<ClusterId, ClusterInformation> messageTypesByClusterId = new HashMap<>();
        containers.stream().map(pc -> pc.clusterDefinition).forEach(c -> {
            messageTypesByClusterId.put(c.getClusterId(),
                    new ClusterInformation(c.getRoutingStrategyId(), c.getClusterId(), c.getMessageProcessor().messagesTypesHandled()));
        });
        final NodeInformation nodeInfo = new NodeInformation(receiver.transportTypeId(), nodeAddress, messageTypesByClusterId);

        // Then actually register the Node
        keepNodeRegstered = new PersistentTask(LOGGER, isRunning, persistenceScheduler, RETRY_PERIOND_MILLIS) {
            @Override
            public boolean execute() {
                try {
                    final String application = node.application;
                    session.recursiveMkdir(clusters(application), DirMode.PERSISTENT);
                    session.recursiveMkdir(nodes(application), DirMode.PERSISTENT);

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

        // Also start the inbound side routing strategies
        IntStream.range(0, containers.size()).forEach(i -> {
            final PerContainer c = containers.get(i);
            c.inboundStrategy.setContainerDetails(c.container.getClusterId(), new ContainerAddress(nodeAddress, i));
            c.inboundStrategy.start(this);
        });

        this.rsManager = new RoutingStrategyManager();
        rsManager.start(this);
        this.tManager = new TransportManager();
        tManager.start(this);
        this.router = new Router(rsManager, nodeAddress, nodeReciever, tManager);
        this.router.start(this);
        adaptors.forEach(a -> a.setDispatcher(router));

        containers.forEach(pc -> pc.container.setDispatcher(router));
        // =====================================

        return this;
    }

    private static void stopMe(final AutoCloseable ac) {
        try {
            if (ac != null)
                ac.close();
        } catch (final Exception e) {
            LOGGER.warn("Couldn't close " + ac.getClass().getSimpleName(), e);
        }
    }

    private static void stopMe(final Service ac) {
        try {
            if (ac != null)
                ac.stop();
        } catch (final Exception e) {
            LOGGER.warn("Couldn't close " + ac.getClass().getSimpleName(), e);
        }
    }

    public void stop() {
        isRunning.set(false);
        stopMe(receiver);

        containers.stream().map(pc -> pc.inboundStrategy).forEach(NodeManager::stopMe);

        stopMe(router);
        stopMe(tManager);
        stopMe(rsManager);
    }

    @Override
    public void close() throws Exception {
        stop();
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

    // Testing accessors

    /**
     * STRICTLY FOR TESTING
     */
    public Router getRouter() {
        return router;
    }

    private static class PerContainer {
        final Container container;
        final RoutingStrategy.Inbound inboundStrategy;
        final Cluster clusterDefinition;

        public PerContainer(final Container container, final Inbound inboundStrategy, final Cluster clusterDefinition) {
            this.container = container;
            this.inboundStrategy = inboundStrategy;
            this.clusterDefinition = clusterDefinition;
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

}
