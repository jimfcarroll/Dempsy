package net.dempsy.router;

import net.dempsy.Manager;

public class RoutingStrategyManager extends Manager<RoutingStrategy.Outbound> {
    public RoutingStrategyManager() {
        super(RoutingStrategy.Outbound.class);
    }
}
