package net.dempsy.messages;

import java.lang.reflect.InvocationTargetException;

public interface MessageProcessorLifecycle {

    /**
     * Creates a new instance from the prototype.
     */
    public Object newInstance() throws InvocationTargetException, IllegalAccessException;

    /**
     * Invokes the activation method of the passed instance.
     */
    public boolean activate(Object instance, Object key, byte[] activationData)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the passivation method of the passed instance. Will return the object's passivation data, <code>null</code> if there is none.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public byte[] passivate(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the appropriate message handler of the passed instance. Caller is responsible for not passing <code>null</code> messages.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public Object invoke(Object instance, Object message)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the output method, if it exists. If the instance does not have an annotated output method, this is a no-op (this is simpler than requiring the caller to check every instance).
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public Object invokeOutput(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     * 
     * @param instance
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public boolean invokeEvictable(Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException;

    /**
     * Determines whether this MP provides an output method.
     */
    public boolean isOutputSupported();

    /**
     * Determines whether this MP provides an evictable method.
     */
    public boolean isEvictableSupported();

    public static enum Operation {
        handle,
        output
    };

    /**
     * Provide a description of the invocation for the specified operation. This is used for error messages, exceptions, and logging.
     * 
     * @param op
     * @param message
     * @return
     */
    public String invokeDescription(final Operation op, final Object message);

}
