package net.dempsy;

import net.dempsy.container.Container;
import net.dempsy.messages.MessageProcessorLifecycle;

public class NodeManagerTestUtil {

    public static Router getRouter(final NodeManager nm) {
        return nm.getRouter();
    }

    public static MessageProcessorLifecycle<?> getMp(final NodeManager nm, final String clusterName) {
        return nm.getMp(clusterName);
    }

    public static Container getContainer(final NodeManager nm, final String clusterName) {
        return nm.getContainer(clusterName);
    }

}
