package net.dempsy.transport.passthrough;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;

public class PassthroughReceiver implements Receiver {
    List<Listener<?>> listeners = new CopyOnWriteArrayList<>();

    PassthroughAddress destination = new PassthroughAddress(new PassthroughSender(listeners));

    @Override
    public NodeAddress getAddress() throws MessageTransportException {
        return destination;
    }

    /**
     * A receiver is started with a Listener and a threading model.
     */
    @Override
    public void start(final Listener<?> listener, final ThreadingModel threadingModel) throws MessageTransportException {
        listeners.add(listener);
    }

    @Override
    public void close() throws Exception {}
}
