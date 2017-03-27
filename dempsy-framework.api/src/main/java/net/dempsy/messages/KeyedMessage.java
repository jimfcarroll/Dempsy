package net.dempsy.messages;

public class KeyedMessage {
    public final Object key;
    public final Object message;

    public KeyedMessage(final Object key, final Object message) {
        this.key = key;
        this.message = message;
    }
}
