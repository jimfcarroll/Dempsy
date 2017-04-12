package net.dempsy;

import static net.dempsy.util.Functional.recheck;
import static net.dempsy.util.Functional.uncheck;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Cluster;
import net.dempsy.config.Node;
import net.dempsy.utils.test.SystemPropertyManager;

@Ignore
@RunWith(Parameterized.class)
public class DempsyBaseTest {
    /**
     * Setting 'hardcore' to true causes EVERY SINGLE IMPLEMENTATION COMBINATION to be used in 
     * every runAllCombinations call. This can make tests run for a loooooong time.
     */
    public static boolean hardcore = false;

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
    protected final String sessionFactoryCtx;

    protected static final String ROUTER_ID_PREFIX = "net.dempsy.router.";
    protected static final String CONTAINER_ID_PREFIX = "net.dempsy.container.";
    protected static final String COLLAB_CTX_PREFIX = "classpath:/td/collab-";
    protected static final String COLLAB_CTX_SUFFIX = ".xml";

    protected DempsyBaseTest(final Logger logger, final String routerId, final String containerId,
            final String sessionFactoryCtx) {
        this.LOGGER = logger;
        this.routerId = routerId;
        this.containerId = containerId;
        this.sessionFactoryCtx = sessionFactoryCtx;
    }

    @Parameters(name = "{index}: routerId={0}, container={1}, cluster={2}")
    public static Collection<Object[]> combos() {
        final String[] routers = { "simple", "microshard" };
        final String[] containers = { "locking", "nonlocking", "altnonlocking" };
        final String[] sessions = { "local", "zookeeper" };

        final List<Object[]> ret = new ArrayList<>();
        for (final String router : routers) {
            for (final String container : containers) {
                for (final String sessFact : sessions) {
                    ret.add(new Object[] { router, container, sessFact });
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

    @FunctionalInterface
    public static interface TestToRun {
        public void test(List<NodeManagerWithContext> nodes) throws Exception;
    }

    private static final String[] frameworkCtx = { "classpath:/td/node.xml", "classpath:/td/transport-bq.xml" };

    ServiceTracker currentlyTracking = null;

    protected void stopSystem() throws Exception {
        if (currentlyTracking != null)
            currentlyTracking.close();
        else
            throw new IllegalStateException("Not currently tracking any Dempsy system");
    }

    protected void runCombos(final String[][] ctxs, final TestToRun test) throws Exception {

        try (final ServiceTracker tr = new ServiceTracker()) {
            currentlyTracking = tr;
            tr.track(new SystemPropertyManager())
                    .set("routing-strategy", ROUTER_ID_PREFIX + routerId)
                    .set("container-type", CONTAINER_ID_PREFIX + containerId);

            // instantiate session factory
            final ClusterInfoSessionFactory sessFact = tr
                    .track(new ClassPathXmlApplicationContext(COLLAB_CTX_PREFIX + sessionFactoryCtx + COLLAB_CTX_SUFFIX))
                    .getBean(ClusterInfoSessionFactory.class);

            final List<NodeManagerWithContext> cpCtxs = IntStream.range(0, ctxs.length)
                    .mapToObj(i -> {
                        final List<String> fullCtx = new ArrayList<>(Arrays.asList(ctxs[i]));
                        fullCtx.addAll(Arrays.asList(frameworkCtx));
                        LOGGER.debug("Starting node with " + fullCtx);
                        return tr.track(new ClassPathXmlApplicationContext(fullCtx.toArray(new String[fullCtx.size()])));
                    })
                    .map(ctx -> tr.track(new NodeManagerWithContext(new NodeManager()
                            .node(ctx.getBean(Node.class))
                            .collaborator(uncheck(() -> sessFact.createSession()))
                            .start(), ctx)))
                    .collect(Collectors.toList());

            recheck(() -> cpCtxs.forEach(n -> assertTrue(uncheck(() -> poll(o -> n.manager.isReady())))));

            test.test(cpCtxs);
            currentlyTracking = null;
        }

        LocalClusterSessionFactory.completeReset();
    }
}
