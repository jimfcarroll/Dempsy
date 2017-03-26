package net.dempsy;

import java.util.List;

import net.dempsy.Router.RoutedMessage;
import net.dempsy.container.Container;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;

public class NodeReceiver implements Listener<RoutedMessage> {

    private final Container[] containers;

    public NodeReceiver(final List<Container> nodeContainers) {
        containers = nodeContainers.toArray(new Container[nodeContainers.size()]);
    }

    @Override
    public boolean onMessage(final RoutedMessage message) throws MessageTransportException {
        // TODO: consider if blocking should be configurable by cluster? node? etc.
        return containers[message.container].dispatch(new KeyedMessage(message.key, message.message), true);
    }

}