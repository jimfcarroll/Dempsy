package net.dempsy;

import java.util.Set;

import net.dempsy.config.ClusterId;

public class ClusterInformation {
    public final Set<String> messageTypesHandled;
    public final String routingStrategyTypeId;
    public final ClusterId clusterId;

    public ClusterInformation(final String routingStrategyTypeId, final ClusterId clusterId, final Set<String> messageTypesHandled) {
        this.routingStrategyTypeId = routingStrategyTypeId;
        this.clusterId = clusterId;
        this.messageTypesHandled = messageTypesHandled;
    }
}
