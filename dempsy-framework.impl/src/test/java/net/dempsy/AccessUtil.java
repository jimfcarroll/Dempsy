package net.dempsy;

import net.dempsy.container.Container;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;

public class AccessUtil {

    public static Router getRouter(final NodeManager nm) {
        return nm.getRouter();
    }

    public static MessageProcessorLifecycle<?> getMp(final NodeManager nm, final String clusterName) {
        return nm.getMp(clusterName);
    }

    public static Container getContainer(final NodeManager nm, final String clusterName) {
        return nm.getContainer(clusterName);
    }

    public static boolean canReach(final Router router, final String cluterName, final KeyedMessageWithType message) {
        return router.canReach(cluterName, message);
    }

}
