package net.dempsy.router.microshard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoException.NoNodeException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.ContainerAddress;
import net.dempsy.router.microshard.MicroshardUtils.ShardInfo;
import net.dempsy.util.SafeString;
import net.dempsy.util.executor.AutoDisposeSingleThreadScheduler;
import net.dempsy.utils.PersistentTask;

public class MicroshardingRouter implements RoutingStrategy.Router {
    private static Logger LOGGER = LoggerFactory.getLogger(MicroshardingRouter.class);
    private static final long RETRY_TIMEOUT = 500L;

    private final AtomicReference<ContainerAddress[]> destinations = new AtomicReference<ContainerAddress[]>(null);
    private final ClusterInfoSession session;
    final ClusterId clusterId;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final PersistentTask setupDestinations;
    private final AutoDisposeSingleThreadScheduler dscheduler;
    private final MicroshardUtils msutils;
    private final MicroshardingRouterFactory mommy;
    private final String thisNodeId;

    MicroshardingRouter(final MicroshardingRouterFactory mom, final ClusterId clusterId, final Infrastructure infra) {
        this.mommy = mom;
        this.clusterId = clusterId;
        this.dscheduler = infra.getScheduler();
        this.msutils = new MicroshardUtils(infra.getRootPaths(), clusterId, null);
        this.session = infra.getCollaborator();
        this.thisNodeId = infra.getNodeId();
        this.isRunning.set(true);
        this.setupDestinations = makePersistentTask();
        this.setupDestinations.process();
    }

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        final ContainerAddress[] destinationArr = destinations.get();
        if (destinationArr == null)
            throw new DempsyException("It appears the Outbound strategy for the message key " +
                    SafeString.objectDescription(message != null ? message.key : null)
                    + " is being used prior to initialization or after a failure.");
        final int length = destinationArr.length;
        if (length == 0)
            return null;
        return destinationArr[Math.abs(message.key.hashCode() % length)];
    }

    @Override
    public synchronized void release() {
        mommy.release(this);
        isRunning.set(false);
    }

    @Override
    public Collection<ContainerAddress> allDesintations() {
        // we are only going to consider a destination if it's fully represented.
        final ContainerAddress[] cur = destinations.get();
        if (cur == null)
            return new ArrayList<>();
        final Set<ContainerAddress> ret = Arrays.stream(cur).filter(ca -> ca != null).collect(Collectors.toSet());

        final int nodeCount = ret.size();
        final int min = Math.floorDiv(cur.length, nodeCount);
        final int max = (int) Math.ceil((double) cur.length / (double) nodeCount);

        final ArrayList<ContainerAddress> tmp = new ArrayList<>(ret);
        for (final ContainerAddress addr : tmp) {
            // how many?
            int count = 0;
            for (final ContainerAddress known : cur) {
                if (addr.equals(known))
                    count++;
            }
            if (count < min || count > max)
                ret.remove(addr);
        }

        return ret;
    }

    @Override
    public String toString() {
        return "{" + MicroshardingRouter.class.getSimpleName() + " at " + thisNodeId + " to " + clusterId + "}";
    }

    /**
     * This makes sure all of the destinations are full.
     */
    boolean isReady() {
        final ContainerAddress[] ds = destinations.get();
        if (ds == null)
            return false;
        for (final ContainerAddress d : ds)
            if (d == null)
                return false;
        final boolean ret = ds.length != 0; // this method is only called in tests and this needs to be true there.

        if (ret && LOGGER.isDebugEnabled())
            LOGGER.debug("at {} to {} is Ready " + shorthand(ds), thisNodeId, clusterId);

        return ret;
    }

    private static Set<ContainerAddress> shorthand(final ContainerAddress[] addr) {
        if (addr == null)
            return null;
        return Arrays.stream(addr).collect(Collectors.toSet());
    }

    private PersistentTask makePersistentTask() {
        return new PersistentTask(LOGGER, isRunning, dscheduler, RETRY_TIMEOUT) {
            Transaction tx = new Transaction(msutils.shardTxDirectory, session, this);

            @Override
            public String toString() {
                final String prefix = "setup or reset known destinations for " + MicroshardingRouter.this;
                if (LOGGER.isTraceEnabled()) {
                    final ContainerAddress[] addr = destinations.get();
                    return prefix + " known destinations=" + shorthand(addr);
                } else
                    return prefix;
            }

            @Override
            public synchronized boolean execute() {
                try {
                    if (LOGGER.isTraceEnabled())
                        LOGGER.trace("Resetting Outbound Strategy for {}", MicroshardingRouter.this);

                    tx.watch();

                    // we need to watch the node directory since relying on the transaction doesn't tell me when a node drops out.
                    session.getSubdirs(msutils.clusterNodesDir, this);

                    final Map<Integer, ShardInfo> shardNumbersToShards = new HashMap<Integer, ShardInfo>();
                    final Collection<String> emptyShards = new ArrayList<String>();
                    final int newtotalAddressCounts = msutils.fillMapFromActiveShards(shardNumbersToShards, session, null);

                    // For now if we hit the race condition between when the target Inbound
                    // has created the shard and when it assigns the shard info, we simply claim
                    // we failed.
                    if (newtotalAddressCounts < 0 || emptyShards.size() > 0)
                        return false;

                    if (newtotalAddressCounts == 0 && LOGGER.isInfoEnabled())
                        LOGGER.info("The cluster {} as seen from {} doesn't seem to have keyspace ownership yet.",
                                clusterId, thisNodeId);

                    if (newtotalAddressCounts > 0) {
                        final ContainerAddress[] newDestinations = new ContainerAddress[newtotalAddressCounts];
                        for (final Map.Entry<Integer, ShardInfo> entry : shardNumbersToShards.entrySet())
                            newDestinations[entry.getKey().intValue()] = entry.getValue().destination;

                        destinations.set(newDestinations);
                    } else
                        destinations.set(new ContainerAddress[0]);

                    final boolean ret = destinations.get() != null;

                    if (ret)
                        tx.close();
                    return ret;
                } catch (final NoNodeException e) {
                    destinations.set(null);
                    if (LOGGER.isTraceEnabled())
                        LOGGER.trace("at {} Failed to set up the Outbound for {}: " + e.getLocalizedMessage(), thisNodeId, clusterId);
                } catch (final ClusterInfoException e) {
                    destinations.set(null);
                    LOGGER.info("at {} Failed to set up the Outbound for {}: " + e.getLocalizedMessage(), thisNodeId, clusterId);
                } catch (final RuntimeException rte) {
                    destinations.set(null); // failure means retry but we're not ready.
                    throw rte;
                }
                return false;
            }
        }; // end setupDestinations PersistentTask declaration
    }
}
