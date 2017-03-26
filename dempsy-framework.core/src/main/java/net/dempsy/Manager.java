package net.dempsy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Manager<T> {
    private static Logger LOGGER = LoggerFactory.getLogger(Manager.class);

    private final Map<String, T> registered = new HashMap<>();
    private final Class<T> clazz;

    public Manager(final Class<T> clazz) {
        this.clazz = clazz;
    }

    public T getAssociatedInstance(final String typeId) throws DempsyException {
        LOGGER.trace("Trying to find SenderFactory associated with the transport \"{}\"", typeId);

        T ret = null;

        synchronized (registered) {
            ret = registered.get(typeId);
            if (ret == null) {
                LOGGER.trace(clazz.getSimpleName()
                        + " associated with the transport \"{}\" wasn't already registered. Attempting to create one assuming the transport id is a package name",
                        typeId);
                // try something stupid like assume it's a package name and the sender factory is in that package
                final Reflections reflections = new Reflections(typeId);

                final Set<Class<? extends T>> senderFactoryClasses = reflections.getSubTypesOf(clazz);

                if (senderFactoryClasses != null && senderFactoryClasses.size() > 0) {
                    final Class<? extends T> sfClass = senderFactoryClasses.iterator().next();
                    if (senderFactoryClasses.size() > 1)
                        LOGGER.warn("Multiple SenderFactory implementations in the package \"{}\". Going with {}", typeId,
                                sfClass.getName());

                    try {
                        ret = sfClass.newInstance();

                        registered.put(typeId, ret);
                    } catch (final InstantiationException | IllegalAccessException e) {
                        throw new DempsyException(
                                "Failed to create an instance of the SenderFactory \"" + sfClass.getName() + "\". Is there a default constructor?",
                                e);
                    }
                }
            }
        }

        if (ret == null)
            throw new DempsyException("Couldn't find a SenderFactory registered with transport type id \"" + typeId
                    + "\" and couldn't find an implementing class assuming the transport type id is a package name");

        return ret;
    }

    public void register(final String typeId, final T factory) {
        synchronized (registered) {
            final T oldFactory = registered.put(typeId, factory);

            if (oldFactory != null)
                LOGGER.info("Overridding an already registered " + clazz.getSimpleName() + "  for transport type id {}", typeId);
        }
    }
}
