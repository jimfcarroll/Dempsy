package net.dempsy.router.microshard;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.config.ClusterId;
import net.dempsy.router.RoutingStrategy;
import net.dempsy.router.RoutingStrategy.Router;

public class MicroshardingRouterFactory implements RoutingStrategy.Factory {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroshardingRouterFactory.class);

    private final Map<String, MicroshardingRouter> cache = new HashMap<>();

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
    public synchronized Router getStrategy(final ClusterId clusterId) {
        final MicroshardingRouter ret = new MicroshardingRouter();
        ret.setClusterId(clusterId);
        return ret;
    }

    @Override
    public boolean isReady() {
        // we're always ready
        return true;
    }

}
