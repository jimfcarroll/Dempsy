package net.dempsy.lifecycle.simple;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.function.Supplier;

import net.dempsy.config.ClusterId;
import net.dempsy.messages.MessageProcessorLifecycle;

public class MessageProcessor implements MessageProcessorLifecycle<Mp> {
    private final Supplier<Mp> newMp;
    private final String[] messageTypes;

    public MessageProcessor(final Supplier<Mp> newMp, final String... messageTypes) {
        if (newMp == null)
            throw new IllegalArgumentException("You must provide a Supplier that creates new " + Mp.class.getSimpleName() + "s.");
        this.newMp = newMp;
        this.messageTypes = Arrays.copyOf(messageTypes, messageTypes.length);
    }

    @Override
    public Object newInstance() {
        return newMp.get();
    }

    @Override
    public void activate(final Mp instance, final Object key, final byte[] activationData)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        instance.activate(activationData, key);
    }

    @Override
    public byte[] passivate(final Mp instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        return instance.passivate();
    }

    @Override
    public KeyedMessage[] invoke(final Mp instance, final Object message)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        return instance.handle(message);
    }

    @Override
    public KeyedMessage[] invokeOutput(final Mp instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        return instance.output();
    }

    @Override
    public void invokeEvictable(final Mp instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        instance.evict();
    }

    @Override
    public String[] messagesTypesHandled() {
        return messageTypes;
    }

    @Override
    public void validate() throws IllegalStateException {}

    @Override
    public void start(final ClusterId myCluster) {}

}
