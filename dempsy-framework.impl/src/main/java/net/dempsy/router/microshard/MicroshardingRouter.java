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

    MicroshardingRouter(final MicroshardingRouterFactory mom, final ClusterId clusterId, final Infrastructure infra) {
        this.mommy = mom;
        this.clusterId = clusterId;
        this.dscheduler = infra.getScheduler();
        this.msutils = new MicroshardUtils(infra.getRootPaths(), clusterId, null);
        this.session = infra.getCollaborator();
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
        final ContainerAddress[] cur = destinations.get();
        if (cur == null)
            return new ArrayList<>();
        return Arrays.stream(cur).filter(ca -> ca != null).collect(Collectors.toList());
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

        if (ret) {
            LOGGER.debug("Is Ready " + shorthand(ds));
        }
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
                final String prefix = "setup or reset known destinations for Router to " + clusterId + " from " + MicroshardingRouter.this;
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
                        LOGGER.trace("Resetting Outbound Strategy for cluster " + clusterId + " from " + clusterId);

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

                    if (newtotalAddressCounts == 0)
                        LOGGER.info("The cluster " + SafeString.valueOf(clusterId) + " doesn't seem to have registered any details.");

                    if (newtotalAddressCounts > 0) {
                        final ContainerAddress[] newDestinations = new ContainerAddress[newtotalAddressCounts];
                        for (final Map.Entry<Integer, ShardInfo> entry : shardNumbersToShards.entrySet()) {
                            final ShardInfo shardInfo = entry.getValue();
                            newDestinations[entry.getKey()] = shardInfo.destination;
                        }

                        destinations.set(newDestinations);
                    } else
                        destinations.set(new ContainerAddress[0]);

                    final boolean ret = destinations.get() != null;

                    if (ret)
                        tx.close();
                    return ret;
                } catch (final ClusterInfoException e) {
                    destinations.set(null);
                    LOGGER.debug("Failed to set up the Outbound for {}: {}", clusterId, e.getLocalizedMessage());
                } catch (final RuntimeException rte) {
                    destinations.set(null); // failure means retry but we're not ready.
                    throw rte;
                }
                return false;
            }
        }; // end setupDestinations PersistentTask declaration
    }
}
