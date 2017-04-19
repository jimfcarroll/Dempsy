package net.dempsy.transport.passthrough;

import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Receiver;

public class PassthroughReceiver implements Receiver {
    private final PassthroughAddress destination = new PassthroughAddress(new PassthroughSender());

    @Override
    public NodeAddress getAddress() throws MessageTransportException {
        return destination;
    }

    /**
     * A receiver is started with a Listener and a threading model.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void start(final Listener<?> listener, final ThreadingModel threadingModel) throws MessageTransportException {
        destination.setListener((Listener<Object>) listener);
    }

    @Override
    public void close() throws Exception {
        destination.getSender(null).stop();
    }
}
