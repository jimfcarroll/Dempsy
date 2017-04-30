package net.dempsy;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import net.dempsy.container.Container;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.RoutedMessage;

public class NodeReceiver implements Listener<RoutedMessage> {

    private final Container[] containers;
    private final ThreadingModel threadModel;
    private final NodeStatsCollector statsCollector;

    public NodeReceiver(final List<Container> nodeContainers, final ThreadingModel threadModel, final NodeStatsCollector statsCollector) {
        containers = nodeContainers.toArray(new Container[nodeContainers.size()]);
        this.threadModel = threadModel;
        this.statsCollector = statsCollector;
    }

    @Override
    public boolean onMessage(final RoutedMessage message) throws MessageTransportException {
        statsCollector.messageReceived(message);
        feedbackLoop(message);
        return true;
    }

    @Override
    public boolean onMessage(final Supplier<RoutedMessage> supplier) {
        statsCollector.messageReceived(supplier);
        threadModel.submitLimited(new ThreadingModel.Rejectable<Object>() {

            @Override
            public Object call() throws Exception {
                doIt(supplier.get());
                return null;
            }

            @Override
            public void rejected() {
                statsCollector.messageDiscarded(supplier);
            }
        }, true);
        return true;
    }

    private void doIt(final RoutedMessage message) {
        Arrays.stream(message.containers).forEach(container -> containers[container].dispatch(new KeyedMessage(message.key, message.message), true));
    }

    public void feedbackLoop(final RoutedMessage message) {
        threadModel.submitLimited(new ThreadingModel.Rejectable<Object>() {

            @Override
            public Object call() throws Exception {
                doIt(message);
                return null;
            }

            @Override
            public void rejected() {
                statsCollector.messageDiscarded(message);
            }
        }, true);
    }
}