package net.dempsy.messages;

public class KeyedMessage {
    public final Object key;
    public final String[] messageTypes;
    public final Object message;

    public KeyedMessage(final Object key, final Object message, final String... messageTypes) {
        this.key = key;
        this.message = message;
        this.messageTypes = messageTypes;
    }
}
