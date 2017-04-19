package net.dempsy.transport.passthrough;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;

public class PassthroughSender implements Sender {
    private NodeStatsCollector statsCollector = null;
    private Listener<Object> listener = null;
    private boolean isRunning = true;

    void setStatsCollector(final NodeStatsCollector sc) {
        this.statsCollector = sc;
    }

    void setListener(final Listener<Object> listener) {
        this.listener = listener;
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        if (!isRunning) {
            if (statsCollector != null)
                statsCollector.messageNotSent();
            throw new MessageTransportException("send called on stopped PassthroughSender");
        }

        listener.onMessage(message);
        if (statsCollector != null)
            statsCollector.messageSent(message);
    }

    @Override
    public void stop() {
        isRunning = false;
    }
}
