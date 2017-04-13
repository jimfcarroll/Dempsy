package net.dempsy.router.microshard;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.DisruptibleSession;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.cluster.zookeeper.ZookeeperSession;
import net.dempsy.cluster.zookeeper.ZookeeperSessionFactory;
import net.dempsy.cluster.zookeeper.ZookeeperTestServer;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.router.microshard.MicroshardUtils.ShardInfo;
import net.dempsy.router.simple.SimpleRoutingStrategy;
import net.dempsy.serialization.jackson.JsonSerializer;
import net.dempsy.transport.NodeAddress;
import net.dempsy.util.TestInfrastructure;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

@RunWith(Parameterized.class)
public class TestMicroshardingRoutingStrategy {
    static final Logger LOGGER = LoggerFactory.getLogger(TestMicroshardingRoutingStrategy.class);
    Infrastructure infra = null;
    ClusterInfoSession session = null;
    AutoDisposeSingleThreadScheduler sched = null;

    final Consumer<ClusterInfoSession> disruptor;
    final ClusterInfoSessionFactory sessFact;

    private static ZookeeperTestServer zookeeperTestServer = null;
    private static ClusterInfoSessionFactory zookeeperFactory = new ClusterInfoSessionFactory() {
        ZookeeperSessionFactory proxied = null;

        @Override
        public ClusterInfoSession createSession() throws ClusterInfoException {
            if (zookeeperTestServer == null)
                zookeeperTestServer = uncheck(() -> new ZookeeperTestServer());
            if (proxied == null)
                proxied = new ZookeeperSessionFactory(zookeeperTestServer.connectString(), 5000, new JsonSerializer());
            return proxied.createSession();
        }
    };

    @Parameters(name = "{index}: session factory={0}, disruptor={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { new LocalClusterSessionFactory(), "standard", (Consumer<ClusterInfoSession>) s -> ((DisruptibleSession) s).disrupt() },
                { zookeeperFactory, "standard", (Consumer<ClusterInfoSession>) s -> ((DisruptibleSession) s).disrupt() },
                { zookeeperFactory, "session-expire",
                        (Consumer<ClusterInfoSession>) s -> uncheck(() -> zookeeperTestServer.forceSessionExpiration((ZookeeperSession) s)) },
        });
    }

    public TestMicroshardingRoutingStrategy(final ClusterInfoSessionFactory factory, final String disruptorName,
            final Consumer<ClusterInfoSession> disruptor) {
        LOGGER.debug("Running {}", factory.getClass().getSimpleName());
        this.sessFact = factory;
        this.disruptor = disruptor;
    }

    private static Infrastructure makeInfra(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        return new TestInfrastructure(session, sched);
    }

    @Before
    public void setup() throws ClusterInfoException, IOException {
        if (zookeeperTestServer == null) {
            zookeeperTestServer = new ZookeeperTestServer();
        }

        session = sessFact.createSession();
        sched = new AutoDisposeSingleThreadScheduler("test");

        infra = makeInfra(session, sched);
    }

    @After
    public void after() {
        if (session != null)
            session.close();
    }

    @AfterClass
    public static void teardown() {
        if (zookeeperTestServer != null)
            zookeeperTestServer.close();
    }

    @Test
    public void testInboundSimpleHappyPathRegister() throws Exception {
        final int numShardsToExpect = Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS);
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {
            final ClusterId clusterId = new ClusterId("test", "test");
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), clusterId, null);

            assertNotNull(ib);
            assertTrue(MicroshardingInbound.class.isAssignableFrom(ib.getClass()));

            ib.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("test"), 0));
            ib.start(infra);

            assertTrue(waitForShards(session, msutils, numShardsToExpect));
        }
    }

    private static class MutableInt {
        public int val;
    }

    private static void checkForShardDistribution(final ClusterInfoSession session, final MicroshardUtils msutils, final int numShardsToExpect,
            final int numNodes) throws InterruptedException {
        final MutableInt iters = new MutableInt();
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

    @Test
    public void testInboundDoubleHappyPathRegister() throws Exception {
        final int numShardsToExpect = Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS);

        try (final RoutingStrategy.Inbound ib1 = new Manager<>(RoutingStrategy.Inbound.class)
                .getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());
                final RoutingStrategy.Inbound ib2 = new Manager<>(RoutingStrategy.Inbound.class)
                        .getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {
            final ClusterId clusterId = new ClusterId("test", "test");
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), clusterId, null);

            ib1.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("node1"), 0));
            ib1.start(infra);

            ib2.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("node2"), 0));
            try (final ClusterInfoSession session2 = sessFact.createSession();) {
                ib2.start(new TestInfrastructure(session2, infra.getScheduler()));

                assertTrue(waitForShards(session, msutils, numShardsToExpect));

                // if this worked right then numShardsToExpect/2 should be owned by each ... eventually.
                checkForShardDistribution(session, msutils, numShardsToExpect, 2);

                // disrupt the session. This should cause a reshuffle but not fail
                disruptor.accept(session2);

                // everything should settle back
                checkForShardDistribution(session, msutils, numShardsToExpect, 2);

                // now kill the second session.
                session2.close(); // this will disconnect the second Inbound and so the first should take over

                // see if we now have 1 session and it has all shards
                checkForShardDistribution(session, msutils, numShardsToExpect, 1);
            }
        }
    }

    @Test
    public void testInboundResillience() throws Exception {
        final int numShardsToExpect = Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS);
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {
            final ClusterId clusterId = new ClusterId("test", "test");
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), clusterId, null);

            ib.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("theOnlyNode"), 0));
            ib.start(infra);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);

            disruptor.accept(session);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);
        }
    }

    @Test
    public void testInboundWithOutbound() throws Exception {
        final int numShardsToExpect = Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS);
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {
            final ClusterId cid = new ClusterId("test", "test");
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), cid, null);

            ib.setContainerDetails(cid, new ContainerAddress(new DummyNodeAddress("here"), 0));
            ib.start(infra);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);

            try (final ClusterInfoSession ses2 = sessFact.createSession()) {
                try (final RoutingStrategyManager obman = chain(new RoutingStrategyManager(), o -> o.start(makeInfra(ses2, sched)));
                        final RoutingStrategy.Factory obf = obman.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {

                    assertTrue(poll(o -> obf.isReady()));

                    obf.start(makeInfra(ses2, sched));
                    final RoutingStrategy.Router ob = obf.getStrategy(cid);

                    final KeyedMessageWithType km = new KeyedMessageWithType(new Object(), new Object(), "");
                    assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));

                    final ContainerAddress ca = ob.selectDestinationForMessage(km);
                    assertNotNull(ca);

                    assertEquals("here", ((DummyNodeAddress) ca.node).name);

                    // now disrupt the session
                    session.close();

                    // the destination should clear until a new in runs
                    assertTrue(poll(o -> ob.selectDestinationForMessage(km) == null));

                    try (ClusterInfoSession ses3 = sessFact.createSession();
                            RoutingStrategy.Inbound ib2 = manager.getAssociatedInstance(SimpleRoutingStrategy.class.getPackage().getName())) {
                        ib2.setContainerDetails(cid, ca);
                        ib.start(makeInfra(ses3, sched));

                        assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));
                    }
                }
            }
        }
    }

    private static class DummyNodeAddress implements NodeAddress {
        private static final long serialVersionUID = 1L;
        public final String name;

        @SuppressWarnings("unused")
        private DummyNodeAddress() {
            name = null;
        }

        public DummyNodeAddress(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            return name.equals(((DummyNodeAddress) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return "DummyNodeAddress[ " + name + " ]";
        }
    }

    private static boolean waitForShards(final ClusterInfoSession session, final MicroshardUtils utils, final int shardCount)
            throws InterruptedException {
        return poll(o -> {
            try {
                final Collection<String> subdirs = session.getSubdirs(utils.shardsDir, null);
                return subdirs.size() == shardCount;
            } catch (final ClusterInfoException e) {
                return false;
            }
        });
    }
}
