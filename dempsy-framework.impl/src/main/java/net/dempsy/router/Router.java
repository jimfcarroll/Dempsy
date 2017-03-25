package net.dempsy.router;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;

public class Router extends Dispatcher {

    ClusterInfoSession session;

    public Router(final ClusterInfoSession session) {
        this.session = session;
    }

    @Override
    public void dispatch(final KeyedMessage message) {

    }

}
