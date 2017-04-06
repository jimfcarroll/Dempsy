package net.dempsy.router.microshard;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.DisruptibleSession;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.monitoring.basic.BasicNodeStatsCollector;
import net.dempsy.monitoring.basic.BasicStatsCollectorFactory;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.RoutingStrategyManager;
import net.dempsy.router.microshard.MicroshardUtils.ShardInfo;
import net.dempsy.router.simple.SimpleRoutingStrategy;
import net.dempsy.transport.NodeAddress;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;

public class TestMicroshardingRoutingStrategy {
    Infrastructure infra = null;
    LocalClusterSessionFactory sessFact = null;
    ClusterInfoSession session = null;
    AutoDisposeSingleThreadScheduler sched = null;

    private static class TestInfrastructure implements Infrastructure {
        final ClusterInfoSession session;
        final AutoDisposeSingleThreadScheduler sched;
        final BasicStatsCollectorFactory statsFact;
        final BasicNodeStatsCollector nodeStats;

        TestInfrastructure(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
            this.session = session;
            this.sched = sched;
            statsFact = new BasicStatsCollectorFactory();
            nodeStats = new BasicNodeStatsCollector();
        }

        @Override
        public ClusterInfoSession getCollaborator() {
            return session;
        }

        @Override
        public AutoDisposeSingleThreadScheduler getScheduler() {
            return sched;
        }

        @Override
        public RootPaths getRootPaths() {
            return new RootPaths("/application", "/application/nodes", "/application/clusters");
        }

        @Override
        public ClusterStatsCollector getClusterStatsCollector(final ClusterId clusterId) {
            return statsFact.createStatsCollector(clusterId, null);
        }

        @Override
        public Map<String, String> getConfiguration() {
            return new HashMap<>();
        }

        @Override
        public NodeStatsCollector getNodeStatsCollector() {
            return nodeStats;
        }
    }

    private static Infrastructure makeInfra(final ClusterInfoSession session, final AutoDisposeSingleThreadScheduler sched) {
        return new TestInfrastructure(session, sched);
    }

    @Before
    public void setup() {
        sessFact = new LocalClusterSessionFactory();
        session = sessFact.createSession();
        sched = new AutoDisposeSingleThreadScheduler("test");

        infra = makeInfra(session, sched);
    }

    @After
    public void after() {
        if (session != null)
            session.close();
    }

    @Test
    public void testInboundSimpleHappyPathRegister() throws Exception {
        final int numShardsToExpect = Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS);
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {
            final ClusterId clusterId = new ClusterId("test", "test");
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), clusterId);

            assertNotNull(ib);
            assertTrue(MicroshardingInbound.class.isAssignableFrom(ib.getClass()));

            ib.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("test"), 0));
            ib.start(infra);

            assertTrue(waitForShards(session, msutils, numShardsToExpect));
        }
    }

    private static void checkForShardDistribution(final ClusterInfoSession session, final MicroshardUtils msutils, final int numShardsToExpect,
            final int numNodes) throws InterruptedException {
        assertTrue(poll(o -> {
            try {
                final Collection<String> shards = session.getSubdirs(msutils.shardsDir, null);
                if (shards.size() != numShardsToExpect)
                    return false;
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

                if (counts.size() != numNodes)
                    return false;
                for (final Map.Entry<NodeAddress, AtomicInteger> entry : counts.entrySet()) {
                    if (entry.getValue().get() != (numShardsToExpect / numNodes))
                        return false;
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
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), clusterId);

            ib1.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("node1"), 0));
            ib1.start(infra);

            ib2.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("node2"), 0));
            try (final ClusterInfoSession session2 = new LocalClusterSessionFactory().createSession();) {
                ib2.start(new TestInfrastructure(session2, infra.getScheduler()));

                assertTrue(waitForShards(session, msutils, numShardsToExpect));

                // if this worked right then numShardsToExpect/2 should be owned by each ... eventually.
                checkForShardDistribution(session, msutils, numShardsToExpect, 2);

                // disrupt the session. This should cause a reshuffle but not fail
                ((DisruptibleSession) session2).disrupt();

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
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), clusterId);

            ib.setContainerDetails(clusterId, new ContainerAddress(new DummyNodeAddress("theOnlyNode"), 0));
            ib.start(infra);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);

            ((DisruptibleSession) session).disrupt();

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);
        }
    }

    @Test
    public void testInboundWithOutbound() throws Exception {
        final int numShardsToExpect = Integer.parseInt(MicroshardingInbound.DEFAULT_TOTAL_SHARDS);
        final Manager<RoutingStrategy.Inbound> manager = new Manager<>(RoutingStrategy.Inbound.class);
        try (final RoutingStrategy.Inbound ib = manager.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());) {
            final ClusterId cid = new ClusterId("test", "test");
            final MicroshardUtils msutils = new MicroshardUtils(infra.getRootPaths(), cid);

            ib.setContainerDetails(cid, new ContainerAddress(new DummyNodeAddress("here"), 0));
            ib.start(infra);

            checkForShardDistribution(session, msutils, numShardsToExpect, 1);

            try (final RoutingStrategyManager obman = new RoutingStrategyManager();
                    final RoutingStrategy.Factory obf = obman.getAssociatedInstance(MicroshardingInbound.class.getPackage().getName());
                    final RoutingStrategy.Router ob = obf.getStrategy(cid)) {

                ob.setClusterId(cid);
                ob.start(infra);

                assertTrue(poll(o -> ob.isReady()));

                try (final ClusterInfoSession ses2 = sessFact.createSession()) {
                    ob.start(makeInfra(ses2, sched));

                    final KeyedMessageWithType km = new KeyedMessageWithType(new Object(), new Object(), "");
                    assertTrue(poll(o -> ob.selectDestinationForMessage(km) != null));

                    final ContainerAddress ca = ob.selectDestinationForMessage(km);
                    assertNotNull(ca);

                    assertEquals("here", ((DummyNodeAddress) ca.node).name);

                    // now distupt the session
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
