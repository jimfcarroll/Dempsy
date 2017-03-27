package net.dempsy;

import java.util.Arrays;
import java.util.List;

import net.dempsy.Router.RoutedMessage;
import net.dempsy.container.Container;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;

public class NodeReceiver implements Listener<RoutedMessage> {

    private final Container[] containers;
    private final ThreadingModel threadModel;
    private final StatsCollector statsCollector;

    public NodeReceiver(final List<Container> nodeContainers, final ThreadingModel threadModel, final StatsCollector statsCollector) {
        containers = nodeContainers.toArray(new Container[nodeContainers.size()]);
        this.threadModel = threadModel;
        this.statsCollector = statsCollector;
    }

    @Override
    public boolean onMessage(final RoutedMessage message) throws MessageTransportException {
        // TODO: consider if blocking should be configurable by cluster? node? etc.
        Arrays.stream(message.containers).forEach(container -> containers[container].dispatch(new KeyedMessage(message.key, message.message), true));
        return true;
    }

    public void feedbackLoop(final RoutedMessage message) {
        threadModel.submitLimited(new ThreadingModel.Rejectable<Object>() {

            @Override
            public Object call() throws Exception {
                onMessage(message);
                return null;
            }

            @Override
            public void rejected() {
                statsCollector.messageDiscarded(message);
            }
        });
    }

}