package net.dempsy.transport.tcp.nio;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;

public class NioSenderFactory implements SenderFactory {
    private final static Logger LOGGER = LoggerFactory.getLogger(NioSenderFactory.class);

    public static final String CONFIG_KEY_SENDER_THREADS = "send_threads";
    public static final String DEFAULT_SENDER_THREADS = "1";

    public static final String CONFIG_KEY_SENDER_BLOCKING = "send_blocking";
    public static final String DEFAULT_SENDER_BLOCKING = "true";

    public static final String CONFIG_KEY_SENDER_MAX_QUEUED = "send_max_queued";
    public static final String DEFAULT_SENDER_MAX_QUEUED = "1000";

    private final Manager<Serializer> serializerManager = new Manager<Serializer>(Serializer.class);
    private final ConcurrentHashMap<NioAddress, NioSender> senders = new ConcurrentHashMap<>();

    private NodeStatsCollector statsCollector;
    private boolean running = true;

    String nodeId;

    BlockingQueue<NioSender> queue;
    int maxNumberOfQueuedOutgoing;
    boolean blocking;

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

    }

    public static class Sender implements Runnable {
        final AtomicBoolean isRunning;
        final boolean batch;
        final BlockingQueue<NioSender> queue;

        Sender(final BlockingQueue<NioSender> queue, final AtomicBoolean isRunning, final boolean batch) {
            this.batch = batch;
            this.isRunning = isRunning;
            this.queue = queue;
        }

        @Override
        public void run() {
            while (isRunning.get()) {
                try {
                    final NioSender sender  = batch ? queue.poll() : queue.take();
                    sender.queue
                }
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
                    return senders.computeIfAbsent(tcpaddr, a -> new NioSender(a, this, statsCollector, serializerManager, blocking, queue));
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

        maxNumberOfQueuedOutgoing = Integer.parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_MAX_QUEUED, DEFAULT_SENDER_MAX_QUEUED));

        blocking = Boolean.parseBoolean(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_BLOCKING, DEFAULT_SENDER_BLOCKING));

        this.queue = blocking ? new ArrayBlockingQueue<>(maxNumberOfQueuedOutgoing) : new LinkedBlockingQueue<>();
    }

    void imDone(final NioAddress tcp) {
        senders.remove(tcp);
    }

}
