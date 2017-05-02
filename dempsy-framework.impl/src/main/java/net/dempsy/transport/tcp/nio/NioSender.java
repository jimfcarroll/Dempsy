package net.dempsy.transport.tcp.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.transport.tcp.netty.NettySender;
import net.dempsy.transport.tcp.nio.NioSenderFactory.Sending;
import net.dempsy.util.SpinLock;
import net.dempsy.util.io.MessageBufferOutput;

public final class NioSender implements Sender {
    private final static ConcurrentLinkedQueue<MessageBufferOutput> pool = new ConcurrentLinkedQueue<>();

    private final static Logger LOGGER = LoggerFactory.getLogger(NettySender.class);

    private final NodeStatsCollector statsCollector;
    private final NioAddress addr;
    private final NioSenderFactory owner;

    public final Serializer serializer;

    private final boolean isRunning = true;
    private final boolean blocking;
    final SocketChannel channel;
    BlockingQueue<Object> messages;

    final SpinLock mine = new SpinLock();

    private final AtomicReference<Sending> threadOwner = new AtomicReference<Sending>(null);

    void clearThreadOwner(final Sending curOwner) {
        int numTries = 0;
        while (!threadOwner.compareAndSet(curOwner, null) && isRunning) {
            numTries++;
            if (numTries > 1000) {
                try {
                    Thread.sleep(1);
                } catch (final InterruptedException ie) {}
            } else
                Thread.yield();
        }
    }

    NioSender(final NioAddress addr, final NioSenderFactory parent, final NodeStatsCollector statsCollector,
            final Manager<Serializer> serializerManger, final boolean blocking) throws IOException {
        this.addr = addr;
        serializer = serializerManger.getAssociatedInstance(addr.serializerId);
        this.statsCollector = statsCollector;
        this.owner = parent;
        this.blocking = blocking;

        channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.connect(new InetSocketAddress(addr.inetAddress, addr.port));
        channel.configureBlocking(false);
    }

    @Override
    public void send(final Object message) throws MessageTransportException {

        final Sending curSending = threadOwner.getAndSet(null);
        try {
            // TODO: something good.
        } finally {
            if (curSending != null)
                threadOwner.set(curSending);
        }

    }

    @Override
    public synchronized void stop() {
        // TODO: something
    }

}