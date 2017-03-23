package net.dempsy.messages;

import java.lang.reflect.InvocationTargetException;

import net.dempsy.config.ClusterId;

public interface MessageProcessorLifecycle<T> {

    public static class KeyedMessage {
        public final Object key;
        public final String[] messageTypes;
        public final Object message;

        public KeyedMessage(final Object key, final Object message, final String... messageTypes) {
            this.key = key;
            this.message = message;
            this.messageTypes = messageTypes;
        }
    }

    /**
     * Creates a new instance from the prototype.
     */
    public Object newInstance() throws InvocationTargetException, IllegalAccessException;

    /**
     * Invokes the activation method of the passed instance.
     */
    public void activate(T instance, Object key, byte[] activationData)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the passivation method of the passed instance. Will return the object's passivation data, <code>null</code> if there is none.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public byte[] passivate(T instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the appropriate message handler of the passed instance. Caller is responsible for not passing <code>null</code> messages.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public KeyedMessage[] invoke(T instance, Object message)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the output method, if it exists. If the instance does not have an annotated output method, this is a no-op (this is simpler than requiring the caller to check every instance).
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public KeyedMessage[] invokeOutput(T instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     * 
     * @param instance
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public void invokeEvictable(T instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    public String[] messagesTypesHandled();

    public void validate() throws IllegalStateException;

    public void start(ClusterId myCluster);
}
