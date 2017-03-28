package net.dempsy.router.simple;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.Outbound;

public class SimpleRoutingStrategyFactory implements RoutingStrategy.Factory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRoutingStrategyFactory.class);

    private final Map<String, SimpleRoutingStrategy> cache = new HashMap<>();

    @Override
    public void start(final Infrastructure infra) {}

    @Override
    public synchronized void stop() {
        cache.values().forEach(s -> {
            try {
                s.stop();
            } catch (final RuntimeException rte) {
                LOGGER.error("Failure shutting down routing strategy", rte);
            }
        });
        cache.clear();
    }

    @Override
    public synchronized Outbound getStrategy(final ClusterId clusterId) {
        final SimpleRoutingStrategy ret = new SimpleRoutingStrategy();
        ret.setClusterId(clusterId);
        return ret;
    }

    @Override
    public boolean isReady() {
        // we're always ready
        return true;
    }

}
