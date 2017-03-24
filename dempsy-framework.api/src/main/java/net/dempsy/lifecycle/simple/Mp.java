package net.dempsy.lifecycle.simple;

import net.dempsy.messages.KeyedMessage;

public interface Mp {
    public KeyedMessage[] handle(KeyedMessage message);

    public default boolean evictable() {
        return false;
    }

    public default boolean shouldBeEvicted() {
        return false;
    }

    public default boolean isEvictable() {
        return false;
    }

    public default KeyedMessage[] output() {
        return null;
    }

    public default byte[] passivate() {
        return null;
    }

    public default void activate(final byte[] data, final Object key) {}
}
