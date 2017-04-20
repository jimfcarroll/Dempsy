package net.dempsy;

import static net.dempsy.util.Functional.recheck;
import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.Infrastructure.RootPaths;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.ClusterId;
import net.dempsy.config.Node;
import net.dempsy.router.microshard.MicroshardUtils;
import net.dempsy.router.microshard.MicroshardUtils.ShardInfo;
import net.dempsy.router.microshard.MicroshardingInbound;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.blockingqueue.BlockingQueueAddress;
import net.dempsy.utils.test.SystemPropertyManager;

@Ignore
@RunWith(Parameterized.class)
public class DempsyBaseTest {
    // /**
    // * Setting 'hardcore' to true causes EVERY SINGLE IMPLEMENTATION COMBINATION to be used in
    // * every runAllCombinations call. This can make tests run for a loooooong time.
    // */
    // public static boolean hardcore = false;

    protected Logger LOGGER;

    /**
     * Contains a generic node specification. It needs to following variables set:
     * 
     * <ul>
     * <li>routing-strategy - set the to the routing strategy id. </li>
     * <li>container-type - set to the container type id </li>
     * </ul>
     * 
     * <p>It autowires all of the {@link Cluster}'s that appear in the same context.</p>
     * 
     * <p>Currently it directly uses the {@code BasicStatsCollector}</p>
     */
    public static String nodeCtx = "classpath:/td/node.xml";

    protected final String routerId;
    protected final String containerId;
    protected final String sessionType;
    protected final String transportType;

    protected static final String ROUTER_ID_PREFIX = "net.dempsy.router.";
    protected static final String CONTAINER_ID_PREFIX = "net.dempsy.container.";
    protected static final String COLLAB_CTX_PREFIX = "classpath:/td/collab-";
    protected static final String COLLAB_CTX_SUFFIX = ".xml";
    protected static final String TRANSPORT_CTX_PREFIX = "classpath:/td/transport-";
    protected static final String TRANSPORT_CTX_SUFFIX = ".xml";

    protected DempsyBaseTest(final Logger logger, final String routerId, final String containerId,
            final String sessionType, final String transportType) {
        this.LOGGER = logger;
        this.routerId = routerId;
        this.containerId = containerId;
        this.sessionType = sessionType;
        this.transportType = transportType;
    }

    @Parameters(name = "{index}: routerId={0}, container={1}, cluster={2}, transport={3}")
    public static Collection<Object[]> combos() {
        final String[] routers = { "simple", "microshard" };
        final String[] containers = { "locking", "nonlocking", "altnonlocking" };
        final String[] sessions = { "local", "zookeeper" };
        final String[] transports = { "bq", "passthrough", "netty" };

        final List<Object[]> ret = new ArrayList<>();
        for (final String router : routers) {
            for (final String container : containers) {
                for (final String sessFact : sessions) {
                    for (final String tp : transports) {
                        ret.add(new Object[] { router, container, sessFact, tp });
                    }
                }
            }
        }
        return ret;
    }

    public static class NodeManagerWithContext implements AutoCloseable {
        public final NodeManager manager;
        public final ClassPathXmlApplicationContext ctx;

        public NodeManagerWithContext(final NodeManager manager, final ClassPathXmlApplicationContext ctx) {
            this.manager = manager;
            this.ctx = ctx;
        }

        @Override
        public void close() throws Exception {
            ctx.close();
            manager.close();
        }
    }

    public static class Nodes {
        public final List<NodeManagerWithContext> nodes;
        public final ClusterInfoSessionFactory sessionFactory;

        public Nodes(final List<NodeManagerWithContext> nodes, final ClusterInfoSessionFactory sessionFactory) {
            this.nodes = nodes;
            this.sessionFactory = sessionFactory;
        }
    }

    @FunctionalInterface
    public static interface TestToRun {
        public void test(Nodes nodes) throws Exception;
    }

    @FunctionalInterface
    public static interface ComboFilter {
        public boolean filter(final String routerId, final String containerId, final String sessionType, final String transportId);
    }

    private static final String[] frameworkCtx = { "classpath:/td/node.xml" };

    ServiceTracker currentlyTracking = null;
    ClusterInfoSessionFactory currentSessionFactory = null;

    protected void stopSystem() throws Exception {
        currentSessionFactory = null;
        if (currentlyTracking != null)
            currentlyTracking.close();
        else
            throw new IllegalStateException("Not currently tracking any Dempsy system");

    }

    protected void runCombos(final String testName, final String[][] ctxs, final TestToRun test) throws Exception {
        runCombos(testName, null, ctxs, test);
    }

    private static AtomicLong runComboSequence = new AtomicLong(0);

    protected void runCombos(final String testName, final ComboFilter filter, final String[][] ctxs, final TestToRun test) throws Exception {
        if (filter != null && !filter.filter(routerId, containerId, sessionType, transportType))
            return;

        try (final ServiceTracker tr = new ServiceTracker()) {
            currentlyTracking = tr;
            tr.track(new SystemPropertyManager())
                    .set("routing-strategy", ROUTER_ID_PREFIX + routerId)
                    .set("container-type", CONTAINER_ID_PREFIX + containerId)
                    .set("test-name", testName + "-" + runComboSequence.getAndIncrement());

            // instantiate session factory
            final ClusterInfoSessionFactory sessFact = tr
                    .track(new ClassPathXmlApplicationContext(COLLAB_CTX_PREFIX + sessionType + COLLAB_CTX_SUFFIX))
                    .getBean(ClusterInfoSessionFactory.class);

            currentSessionFactory = sessFact;

            final List<NodeManagerWithContext> cpCtxs = IntStream.range(0, ctxs.length)
                    .mapToObj(i -> makeNode(ctxs[i]))
                    .collect(Collectors.toList());

            recheck(() -> cpCtxs.forEach(n -> assertTrue(uncheck(() -> poll(o -> n.manager.isReady())))), InterruptedException.class);

            test.test(new Nodes(cpCtxs, sessFact));
            currentlyTracking = null;
        }

        LocalClusterSessionFactory.completeReset();
        BlockingQueueAddress.completeReset();
    }

    @SuppressWarnings("resource")
    protected NodeManagerWithContext makeNode(final String[] ctxArr) {
        final List<String> fullCtx = new ArrayList<>(Arrays.asList(ctxArr));
        fullCtx.addAll(Arrays.asList(frameworkCtx));
        fullCtx.add(TRANSPORT_CTX_PREFIX + transportType + TRANSPORT_CTX_SUFFIX);
        LOGGER.debug("Starting node with " + fullCtx);
        final ClassPathXmlApplicationContext ctx = currentlyTracking
                .track(new ClassPathXmlApplicationContext(fullCtx.toArray(new String[fullCtx.size()])));

        return currentlyTracking.track(new NodeManagerWithContext(new NodeManager()
                .node(ctx.getBean(Node.class))
                .collaborator(uncheck(() -> currentSessionFactory.createSession()))
                .start(), ctx));
    }

    private static class MutableInt {
        public int val;
    }

    protected void waitForEvenShardDistribution(final ClusterInfoSession session, final String cluster, final int numNodes)
            throws InterruptedException {
        waitForEvenShardDistribution(session, cluster, Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS), numNodes);
    }

    protected void waitForEvenShardDistribution(final ClusterInfoSession session, final String cluster, final int numShardsToExpect,
            final int numNodes) throws InterruptedException {
        final MutableInt iters = new MutableInt();
        final String testName = System.getProperty("test-name");
        if (testName == null)
            throw new RuntimeException("test-name system property isn't set. Don't know the application name");
        final ClusterId clusterId = new ClusterId(testName, cluster);
        final MicroshardUtils msutils = new MicroshardUtils(new RootPaths(clusterId.applicationName), clusterId, null);
        assertTrue(poll(o -> {
            try {
                iters.val++;
                final boolean showLog = LOGGER.isTraceEnabled() && (iters.val % 100 == 0);
                final Collection<String> shards = session.getSubdirs(msutils.shardsDir, null);
                if (shards.size() != numShardsToExpect) {
                    if (showLog)
                        LOGGER.trace("Not all shards available. Expecting {} but got {}", numShardsToExpect, shards.size());
                    return false;
                }
                final Map<NodeAddress, AtomicInteger> counts = new HashMap<>();
                for (final String subdir : shards) {
                    final ShardInfo si = (ShardInfo) session.getData(msutils.shardsDir + "/" + subdir, null);
                    AtomicInteger count = counts.get(si.destination.node);
                    if (count == null) {
                        count = new AtomicInteger(0);
                        counts.put(si.destination.node, count);
                    }
                    count.incrementAndGet();
                }

                if (counts.size() != numNodes) {
                    if (showLog)
                        LOGGER.trace("Not all nodes registered. {} out of {}", counts.size(), numNodes);
                    return false;
                }
                for (final Map.Entry<NodeAddress, AtomicInteger> entry : counts.entrySet()) {
                    if (Math.abs(entry.getValue().get() - (numShardsToExpect / numNodes)) > 1) {
                        if (showLog)
                            LOGGER.trace("Counts for {} is below what's expected. {} is not 1 away from " + (numShardsToExpect / numNodes) + ")",
                                    entry.getKey(), entry.getValue());
                        return false;
                    }
                }
                return true;
            } catch (final ClusterInfoException cie) {
                return false;
            }
        }));

    }

}
