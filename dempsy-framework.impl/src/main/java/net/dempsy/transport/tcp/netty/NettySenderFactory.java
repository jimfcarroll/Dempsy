package net.dempsy.transport.tcp.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import net.dempsy.Infrastructure;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.tcp.TcpAddress;

public class NettySenderFactory implements SenderFactory {
    private final ConcurrentHashMap<TcpAddress, NettySender> senders = new ConcurrentHashMap<>();
    private NodeStatsCollector statsCollector;
    private boolean running = true;
    String nodeId;

    @Override
    public void close() {
        running = false;
        final List<NettySender> snapshot = new ArrayList<>(senders.values());
        snapshot.forEach(s -> s.stop());
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public NettySender getSender(final NodeAddress destination) throws MessageTransportException {
        NettySender ret = senders.get(destination);
        if (ret == null) {
            if (running) {
                final TcpAddress tcpaddr = (TcpAddress) destination;
                ret = new NettySender(tcpaddr, this, statsCollector);
                final NettySender tmp = senders.putIfAbsent(tcpaddr, ret);
                if (tmp != null) {
                    ret.stop();
                    ret = tmp;
                }
            } else {
                throw new IllegalStateException(NettySenderFactory.class.getSimpleName() + " is stopped.");
            }
        }
        return ret;
    }

    @Override
    public void start(final Infrastructure infra) {
        this.statsCollector = infra.getNodeStatsCollector();
        this.nodeId = infra.getNodeId();
    }

    void imDone(final TcpAddress tcp) {
        senders.remove(tcp);
    }
}
