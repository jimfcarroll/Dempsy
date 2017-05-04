package net.dempsy.transport.tcp.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.transport.tcp.netty.NettySender;
import net.dempsy.transport.tcp.nio.internal.NioUtils;

public final class NioSender implements Sender {
    private final static Logger LOGGER = LoggerFactory.getLogger(NettySender.class);

    private final NodeStatsCollector statsCollector;
    private final TcpAddress addr;
    private final NioSenderFactory owner;
    private final String nodeId;

    public final Serializer serializer;

    final SocketChannel channel;

    private boolean connected = false;
    private int sendBufferSize = -1;
    private int recvBufferSize = -1;

    // read from Sending
    BlockingQueue<Object> messages;
    boolean running = true;

    NioSender(final TcpAddress addr, final NioSenderFactory parent) {
        this.owner = parent;
        this.addr = addr;
        serializer = parent.serializerManager.getAssociatedInstance(addr.serializerId);
        this.statsCollector = parent.statsCollector;
        this.nodeId = parent.nodeId;

        // messages = new LinkedBlockingQueue<>();
        messages = new ArrayBlockingQueue<>(2);
        try {
            channel = SocketChannel.open();
        } catch (final IOException e) {
            throw new MessageTransportException(e); // this is probably impossible
        }
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        boolean done = false;
        while (running && !done) {
            try {
                if (running) {
                    messages.put(message);
                }
                done = true;
            } catch (final InterruptedException e) {

            }
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        try {
            Thread.sleep(1);
        } catch (final InterruptedException e) {}

        final List<Object> drainTo = new ArrayList<>();
        messages.drainTo(drainTo);

        try {
            messages.put(new StopMessage());
        } catch (final InterruptedException e) {
            LOGGER.error("Failed to put a StopMessage in what should have been an empty queue.");
            NioUtils.closeQueitly(channel, LOGGER, nodeId + " sender failed while closing. Ignoring.");
        }

        drainTo.forEach(o -> statsCollector.messageNotSent());
        owner.imDone(addr);
    }

    static class StopMessage {}

    void connect() throws IOException {
        if (!connected) {
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress(addr.inetAddress, addr.port));
            channel.configureBlocking(false);
            sendBufferSize = channel.socket().getSendBufferSize();
            recvBufferSize = addr.recvBufferSize;
            connected = true;
            owner.working.putIfAbsent(this, this);
        }
    }

    int getMaxBatchSize() {
        int ret;
        if (recvBufferSize <= 0)
            ret = sendBufferSize;
        else if (sendBufferSize <= 0)
            ret = recvBufferSize;
        else ret = Math.min(recvBufferSize, sendBufferSize);
        if (ret <= 0) {
            LOGGER.warn("Couldn't determin send and receieve buffer sizes. Setting batch size to ");
            ret = owner.mtu;
        }
        return Math.min(ret, owner.mtu);
    }
}