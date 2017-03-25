package net.dempsy.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;

public class TransportManager {
    private static Logger LOGGER = LoggerFactory.getLogger(TransportManager.class);

    private final Map<String, SenderFactory> registered = new HashMap<>();

    public SenderFactory getAssociatedSenderFactory(final String transportTypeId) throws DempsyException {
        LOGGER.trace("Trying to find SenderFactory associated with the transport \"{}\"", transportTypeId);

        SenderFactory ret = null;

        synchronized (registered) {
            ret = registered.get(transportTypeId);
            if (ret == null) {
                LOGGER.trace(
                        "SenderFactory associated with the transport \"%s\" wasn't already registered. Attempting to create one assuming the transport id is a package name",
                        transportTypeId);
                // try something stupid like assume it's a package name and the sender factory is in that package
                final Reflections reflections = new Reflections("my.package");

                final Set<Class<? extends SenderFactory>> senderFactoryClasses = reflections.getSubTypesOf(SenderFactory.class);

                if (senderFactoryClasses != null && senderFactoryClasses.size() > 0) {
                    final Class<? extends SenderFactory> sfClass = senderFactoryClasses.iterator().next();
                    if (senderFactoryClasses.size() > 1)
                        LOGGER.warn("Multiple SenderFactory implementations in the package \"{}\". Going with {}", transportTypeId,
                                sfClass.getName());

                    try {
                        ret = sfClass.newInstance();

                        registered.put(transportTypeId, ret);
                    } catch (final InstantiationException | IllegalAccessException e) {
                        throw new DempsyException(
                                "Failed to create an instance of the SenderFactory \"" + sfClass.getName() + "\". Is there a default constructor?",
                                e);
                    }
                }
            }

        }
        if (ret == null)
            throw new DempsyException("Couldn't find a SenderFactory registered with transport type id \"" + transportTypeId
                    + "\" and couldn't find an implementing class assuming the transport type id is a package name");

        return ret;
    }

    public TransportManager register(final String transportTypeId, final SenderFactory factory) {
        synchronized (registered) {
            final SenderFactory oldFactory = registered.put(transportTypeId, factory);

            if (oldFactory != null)
                LOGGER.info("Overridding an already registered SenderFactory for transport type id {}", transportTypeId);
        }
        return this;
    }
}
