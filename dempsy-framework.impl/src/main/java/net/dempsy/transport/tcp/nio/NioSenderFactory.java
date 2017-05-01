package net.dempsy.transport.tcp.nio;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;

public class NioSenderFactory implements SenderFactory {
    private final static Logger LOGGER = LoggerFactory.getLogger(NioSenderFactory.class);

    public static final String CONFIG_KEY_SENDER_THREADS = "num_sender_threads";
    public static final String DEFAULT_SENDER_THREADS = "1";

    private final Manager<Serializer> serializerManager = new Manager<Serializer>(Serializer.class);
    private final ConcurrentHashMap<NioAddress, NioSender> senders = new ConcurrentHashMap<>();

    private NodeStatsCollector statsCollector;
    private boolean running = true;
    private EventLoopGroup group = null;

    String nodeId;

    @Override
    public void close() {
        final List<NioSender> snapshot;
        synchronized (this) {
            running = false;
            snapshot = new ArrayList<>(senders.values());
        }
        snapshot.forEach(s -> s.stop());

        // we SHOULD be all done.
        final boolean recurse;
        synchronized (this) {
            recurse = senders.size() > 0;
        }
        if (recurse)
            close();

        final EventLoopGroup tmpGr = group;
        group = null;
        if (tmpGr != null) {
            try {
                tmpGr.shutdownNow();
            } catch (final Exception e) {
                LOGGER.warn("Unexpected exception shutting down netty group", e);
                startGroupStopThread(tmpGr, "netty-sender-group-closer" + threadNum.getAndIncrement() + ") from (" + nodeId + ")");
            }
        }

    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public NioSender getSender(final NodeAddress destination) throws MessageTransportException {
        final NioSender ret = senders.get(destination);
        if (ret == null) {
            synchronized (this) {
                if (running) {
                    final NioAddress tcpaddr = (NioAddress) destination;
                    return senders.computeIfAbsent(tcpaddr, a -> new NioSender(a, this, statsCollector, serializerManager, group));
                } else {
                    throw new IllegalStateException(NioSender.class.getSimpleName() + " is stopped.");
                }
            }
        }
        return ret;
    }

    private final AtomicLong threadNum = new AtomicLong(0L);

    @Override
    public void start(final Infrastructure infra) {
        this.statsCollector = infra.getNodeStatsCollector();
        this.nodeId = infra.getNodeId();

        final int numSenderThreads = Integer
                .parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_THREADS, DEFAULT_SENDER_THREADS));

        this.group = new NioEventLoopGroup(numSenderThreads,
                (ThreadFactory) r -> new Thread(r, "netty-sender-" + threadNum.getAndIncrement() + " from (" + nodeId + ")"));
    }

    void imDone(final NioAddress tcp) {
        senders.remove(tcp);
    }

    @SuppressWarnings("deprecation")
    private static void persistentStopGroup(final EventLoopGroup tmpGr) {
        if (tmpGr == null)
            return;

        int numTries = 0;
        while (!tmpGr.isShutdown() && numTries < 10) {
            try {
                numTries++;
                tmpGr.shutdownNow();
            } catch (final Exception ee) {
                LOGGER.warn("Unexpected exception shutting down netty group", ee);
            }
        }
    }

    private static void startGroupStopThread(final EventLoopGroup tmpGr, final String threadName) {
        // close the damn thing in another thread insistently
        new Thread(() -> {
            persistentStopGroup(tmpGr);
        }, threadName).start();
    }
}
