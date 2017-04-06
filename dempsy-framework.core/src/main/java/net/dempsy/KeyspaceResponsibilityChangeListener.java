package net.dempsy;

import net.dempsy.router.RoutingStrategy;

/**
 * Since the responsibility for the portion of the keyspace that this node is responsible for
 * is determined by the Inbound strategy, when that responsibility changes, Dempsy itself
 * needs to be notified.
 */
public interface KeyspaceResponsibilityChangeListener {
    public void keyspaceResponsibilityChanged(boolean less, boolean more);

    public void setInboundStrategy(RoutingStrategy.Inbound inbound);
}
