package net.dempsy.transport.passthrough;

import java.util.List;

import net.dempsy.monitoring.StatsCollector;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.util.io.MessageBufferInput;
import net.dempsy.util.io.MessageBufferOutput;

public class PassthroughSender implements Sender {
    StatsCollector statsCollector = null;
    List<Listener<?>> listeners;
    boolean isRunning = true;

    private PassthroughSender(final List<Listener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void send(final MessageBufferOutput message) throws MessageTransportException {
        final MessageBufferInput messageBytes = new MessageBufferInput(message.getBuffer(), 0, message.getPosition());
        if (!isRunning) {
            if (statsCollector != null)
                statsCollector.messageNotSent(messageBytes);
            throw new MessageTransportException("send called on stopped PassthroughSender");
        }

        for (final Listener listener : listeners) {
            if (statsCollector != null)
                statsCollector.messageSent(messageBytes.available());
            listener.onMessage(messageBytes, failFast);
        }
    }

}
