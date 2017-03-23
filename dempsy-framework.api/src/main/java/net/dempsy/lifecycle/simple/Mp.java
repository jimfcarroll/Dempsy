package net.dempsy.lifecycle.simple;

import net.dempsy.messages.MessageProcessorLifecycle.KeyedMessage;

public interface Mp {
    public KeyedMessage[] handle(Object message);

    public default boolean evictable() {
        return false;
    }

    public default void evict() {}

    public default KeyedMessage[] output() {
        return null;
    }

    public default byte[] passivate() {
        return null;
    }

    public default void activate(final byte[] data, final Object key) {}
}
