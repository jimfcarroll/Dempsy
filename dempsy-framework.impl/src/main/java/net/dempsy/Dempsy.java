package net.dempsy;

import java.util.ArrayList;
import java.util.List;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoWatcher;
import net.dempsy.config.Node;
import net.dempsy.container.Container;
import net.dempsy.messages.Adaptor;
import net.dempsy.router.Router;

public class Dempsy implements ClusterInfoWatcher {

    private Node node = null;
    private ClusterInfoSession session;
    private final List<Container> containers = new ArrayList<>();
    private final List<Adaptor> adaptors = new ArrayList<>();
    private Router router = null;

    public Dempsy node(final Node node) {
        this.node = node;
        return this;
    }

    public Dempsy collaborator(final ClusterInfoSession session) {
        if (session == null)
            throw new NullPointerException("Cannot pass a null collaborator to " + Dempsy.class.getSimpleName());
        if (this.session != null)
            throw new IllegalStateException("Collaborator session is already set on " + Dempsy.class.getSimpleName());
        this.session = session;
        this.router = new Router(session);
        return this;
    }

    public Dempsy start() throws DempsyException {
        validate();

        // set the dispatcher on adaptors and create containers for mp clusters
        node.getClusters().forEach(c -> {
            if (c.isAdaptor()) {
                final Adaptor adaptor = c.getAdaptor();
                adaptor.setDispatcher(router);
                adaptors.add(adaptor);
            } else {
                final Container con = new Container(c.getMessageProcessor(), c.getClusterId());
                con.setDispatcher(router);
                containers.add(con);
            }
        });

        // register node with session
        session.mkdir

        return this;
    }

    public Dempsy validate() throws DempsyException {
        if (node == null)
            throw new DempsyException("No node set");

        // if session is non-null, then so is the Router.
        if (session == null)
            throw new DempsyException("There's no collaborator set for this \"" + Dempsy.class.getSimpleName() + "\" ");

        return this;
    }

    @Override
    public void process() {}
}
