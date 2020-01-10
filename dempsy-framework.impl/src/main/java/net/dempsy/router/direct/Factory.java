package net.dempsy.router.direct;

import net.dempsy.Locator;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.Inbound;

public class Factory implements Locator {

    @SuppressWarnings("unchecked")
    @Override
    public <T> T locate(final Class<T> clazz) {
        if(Inbound.class.equals(clazz))
            return (T)new DirectInboundSide();
        else if(RoutingStrategy.Factory.class.equals(clazz))
            return (T)new DirectRoutingStrategyFactory();
        return null;
    }

}
