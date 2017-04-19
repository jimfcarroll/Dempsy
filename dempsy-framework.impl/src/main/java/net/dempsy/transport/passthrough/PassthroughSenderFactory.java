package net.dempsy.transport.passthrough;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;

public class PassthroughSenderFactory implements SenderFactory {
    private NodeStatsCollector statsCollector = null;

    @Override
    public synchronized void stop() {}

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public synchronized Sender getSender(final NodeAddress destination) throws MessageTransportException {
        return ((PassthroughAddress) destination).getSender(statsCollector);
    }

    @Override
    public void setStatsCollector(final NodeStatsCollector statsCollector) {
        this.statsCollector = statsCollector;
    }

}
