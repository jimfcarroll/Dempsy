package net.dempsy.transport.tcp.nio;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.transport.tcp.netty.NettySender;
import net.dempsy.util.io.MessageBufferOutput;

public final class NioSender implements Sender {
    private final static ConcurrentLinkedQueue<MessageBufferOutput> pool = new ConcurrentLinkedQueue<>();

    private final static Logger LOGGER = LoggerFactory.getLogger(NettySender.class);

    private final NodeStatsCollector statsCollector;
    private final NioAddress addr;
    private final Serializer serializer;
    private final NioSenderFactory owner;

    private final boolean isRunning = true;
    private final boolean blocking;
    final BlockingQueue<Object> queue = ;

    NioSender(final NioAddress addr, final NioSenderFactory parent, final NodeStatsCollector statsCollector,
            final Manager<Serializer> serializerManger, final boolean blocking) {
        this.addr = addr;
        serializer = serializerManger.getAssociatedInstance(addr.serializerId);
        this.statsCollector = statsCollector;
        this.owner = parent;
        this.blocking = blocking;
        this.queue = queue;
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        // TODO: something
    }

    @Override
    public synchronized void stop() {
        // TODO: something
    }

}