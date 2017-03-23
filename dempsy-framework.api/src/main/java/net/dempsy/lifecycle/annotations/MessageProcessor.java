/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.lifecycle.annotations;

import static net.dempsy.util.Functional.recheck;
import static net.dempsy.util.Functional.uncheck;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import net.dempsy.config.Cluster;
import net.dempsy.config.ClusterId;
import net.dempsy.lifecycle.annotations.internal.AnnotatedMethodInvoker;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.util.SafeString;

/**
 * This class holds the MP prototype, and supports invocation of MP methods on an instance.
 */
public class MessageProcessor<T> implements MessageProcessorLifecycle<T> {
    private static Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private final Object prototype;
    private final Class<?> mpClass;
    private final String mpClassName;

    private final String toStringValue;

    private final Method cloneMethod;
    private MethodHandle activationMethod;
    private final MethodHandle passivationMethod;
    private final List<Method> outputMethods;
    private final MethodHandle evictableMethod;
    private final AnnotatedMethodInvoker invocationMethods;
    private final Set<Class<?>> stopTryingToSendTheseTypes = Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>());

    private static final AnnotatedMethodInvoker messageKeyGetMethodInvoker = new AnnotatedMethodInvoker(MessageKey.class);

    public MessageProcessor(final T prototype) throws IllegalArgumentException {
        this.prototype = prototype;
        this.mpClass = prototype.getClass();
        this.mpClassName = mpClass.getName();
        this.toStringValue = getClass().getName() + "[" + mpClassName + "]";

        validateAsMP();
        cloneMethod = introspectClone();

        invocationMethods = new AnnotatedMethodInvoker(mpClass, MessageHandler.class);
        final Set<Class<?>> keys = invocationMethods.getMethods().keySet();
        for (final Class<?> key : keys) {
            final Method messageKey = AnnotatedMethodInvoker.introspectAnnotationSingle(key, MessageKey.class);
            activationMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Activation.class),
                    (messageKey == null) ? null : messageKey.getReturnType());
        }
        passivationMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Passivation.class));
        outputMethods = AnnotatedMethodInvoker.introspectAnnotationMultiple(mpClass, Output.class);
        evictableMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Evictable.class));
    }

    /**
     * Creates a new instance from the prototype.
     */
    @Override
    public Object newInstance() throws InvocationTargetException, IllegalAccessException {
        return cloneMethod.invoke(prototype);
    }

    /**
     * Invokes the activation method of the passed instance.
     */
    @Override
    public void activate(final T instance, final Object key, final byte[] activationData)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        activationMethod.invoke(instance, key, activationData);
    }

    /**
     * Returns true if the activation method on this prototype exists and the provided exception can be assigned to one of it's declared checked exceptions.
     */
    public boolean activateCanThrowChecked(final Throwable th) {
        return activationMethod.canThrowCheckedException(th);
    }

    /**
     * Invokes the passivation method of the passed instance. Will return the object's passivation data, <code>null</code> if there is none.
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Override
    public byte[] passivate(final T instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        return (byte[]) passivationMethod.invoke(instance);
    }

    /**
     * Invokes the appropriate message handler of the passed instance. Caller is responsible for not passing <code>null</code> messages.
     * 
     * @throws ContainerException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Override
    public KeyedMessage[] invoke(final T instance, final Object message)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        if (!isMessageSupported(message))
            throw new IllegalArgumentException(mpClassName + ": no handler for messages of type: " + message.getClass().getName());

        return convertToKeyMessage(invocationMethods.invokeMethod(instance, message), message);

    }

    private static void getMessages(final Object message, final List<Object> messages) {
        if (message instanceof Iterable) {
            @SuppressWarnings("rawtypes")
            final Iterator it = ((Iterable) message).iterator();
            while (it.hasNext())
                getMessages(it.next(), messages);
        } else
            messages.add(message);
    }

    public Object getPrototype() {
        return prototype;
    }

    /**
     * Invokes the output method, if it exists. If the instance does not have an annotated output method, this is a no-op (this is simpler than requiring the caller to check every instance).
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Override
    public KeyedMessage[] invokeOutput(final T instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        if (outputMethods == null)
            return null;

        return recheck(() -> outputMethods.stream()
                .map(om -> uncheck(() -> convertToKeyMessage(om.invoke(instance), null)))
                .toArray(KeyedMessage[]::new), InvocationTargetException.class);
    }

    /**
     * Invokes the evictable method on the provided instance. If the evictable is not implemented, returns false.
     * 
     * @param instance
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Override
    public void invokeEvictable(final T instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        if (evictableMethod != null)
            evictableMethod.invoke(instance);
    }

    /**
     * Determines whether the passed class matches the prototype's class.
     */
    public boolean isMatchingClass(final Class<?> klass) {
        return klass.equals(prototype.getClass());
    }

    /**
     * Determines whether this MP has a handler for the passed message. Will walk the message's class hierarchy if there is not an exact match.
     */
    public boolean isMessageSupported(final Object message) {
        return invocationMethods.isValueSupported(message);
    }

    @Override
    public void validate() throws IllegalStateException {
        if (prototype != null) {
            if (!prototype.getClass().isAnnotationPresent(Mp.class))
                throw new IllegalStateException("Attempting to set an instance of \"" +
                        SafeString.valueOfClass(prototype) + "\" within the " +
                        Cluster.class.getSimpleName() +
                        " but it isn't identified as a MessageProcessor. Please annotate the class.");

            final Method[] methods = prototype.getClass().getMethods();

            boolean foundAtLeastOneMethod = false;
            for (final Method method : methods) {
                if (method.isAnnotationPresent(MessageHandler.class)) {
                    foundAtLeastOneMethod = true;
                    break;
                }
            }

            if (!foundAtLeastOneMethod)
                throw new IllegalStateException("No method on the message processor of type \"" +
                        SafeString.valueOfClass(prototype)
                        + "\" is identified as a MessageHandler. Please annotate the appropriate method using @MessageHandler.");

            int startMethods = 0;
            for (final Method method : methods) {
                if (method.isAnnotationPresent(Start.class)) {
                    startMethods++;
                }
            }
            if (startMethods > 1)
                throw new IllegalStateException("Multiple methods on the message processor of type\""
                        + SafeString.valueOf(prototype)
                        + "\" is identified as a Start method. Please annotate at most one method using @Start.");

            final Method[] evictableMethods = prototype.getClass().getMethods();

            boolean foundEvictableMethod = false;
            Method evictableMethod = null;
            for (final Method method : evictableMethods) {
                if (method.isAnnotationPresent(Evictable.class)) {
                    if (foundEvictableMethod) {
                        throw new IllegalStateException("More than one method on the message processor of type \"" +
                                SafeString.valueOfClass(prototype)
                                + "\" is identified as a Evictable. Please annotate the appropriate method using @Evictable.");
                    }
                    foundEvictableMethod = true;
                    evictableMethod = method;
                }
            }
            if (evictableMethod != null) {
                if (evictableMethod.getReturnType() == null || !evictableMethod.getReturnType().isAssignableFrom(boolean.class))
                    throw new IllegalStateException(
                            "Evictable method \"" + SafeString.valueOf(evictableMethod) + "\" on the message processor of type \"" +
                                    SafeString.valueOfClass(prototype)
                                    + "\" should return boolean value. Please annotate the appropriate method using @Evictable.");
            }
        }
    }

    /**
     * To instances are equal if they wrap prototypes of the same class.
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj)
            return true;
        else if (obj instanceof MessageProcessor) {
            final MessageProcessor<?> that = (MessageProcessor<?>) obj;
            return this.prototype.getClass() == that.prototype.getClass();
        } else
            return false;
    }

    @Override
    public final int hashCode() {
        return prototype.getClass().hashCode();
    }

    @Override
    public String toString() {
        return toStringValue;
    }

    @Override
    public String[] messagesTypesHandled() {
        return getAcceptedMessages().stream().map(c -> c.getName()).toArray(String[]::new);
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    private void validateAsMP() throws IllegalArgumentException {
        if (mpClass.getAnnotation(Mp.class) == null)
            throw new IllegalStateException("MP class not annotated as MessageProcessor: " + mpClassName);
    }

    private Method introspectClone() throws IllegalStateException {
        try {
            // we do *NOT* allow inherited implementation
            return mpClass.getDeclaredMethod("clone");
        } catch (final SecurityException e) {
            throw new IllegalStateException("container does not have access to the message processor class \"" + mpClassName + "\"", e);
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException("The message processor class \"" + mpClassName + "\" does not declare the clone() method.");
        }
    }

    /**
     * Class to handle method calls for activation and passivation
     *
     */
    protected class MethodHandle {
        private final Method method;
        private int keyPosition = -1;
        private int binayPosition = -1;
        private int totalArguments = 0;

        public MethodHandle(final Method method) {
            this(method, null);
        }

        public MethodHandle(final Method method, final Class<?> keyClass) {
            this.method = method;
            if (this.method != null) {
                final Class<?>[] parameterTypes = method.getParameterTypes();
                this.totalArguments = parameterTypes.length;
                for (int i = 0; i < parameterTypes.length; i++) {
                    final Class<?> parameter = parameterTypes[i];
                    if (parameter.isArray() && parameter.getComponentType().isAssignableFrom(byte.class)) {
                        this.binayPosition = i;
                    } else if (keyClass != null && parameter.isAssignableFrom(keyClass)) {
                        this.keyPosition = i;
                    }
                }
            }
        }

        public Object invoke(final Object instance, final Object key, final byte[] data)
                throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
            if (this.method != null) {
                final Object[] parameters = new Object[this.totalArguments];
                if (this.keyPosition > -1)
                    parameters[this.keyPosition] = key;
                if (this.binayPosition > -1)
                    parameters[this.binayPosition] = data;
                return this.method.invoke(instance, parameters);
            }
            return null;
        }

        public Object invoke(final Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
            return this.invoke(instance, null, null);
        }

        public Method getMethod() {
            return this.method;
        }

        public boolean canThrowCheckedException(final Throwable th) {
            if (method == null || th == null)
                return false;

            for (final Class<?> cur : method.getExceptionTypes())
                if (cur.isInstance(th))
                    return true;
            return false;
        }
    }

    private KeyedMessage[] convertToKeyMessage(final Object toSend, final Object message) {
        final List<Object> messages = new ArrayList<Object>();
        getMessages(toSend, messages);

        final List<KeyedMessage> ret = new ArrayList<>(messages.size());

        for (final Object msg : messages) {
            final Class<?> messageClass = msg.getClass();

            Object msgKeyValue = null;
            try {
                if (!stopTryingToSendTheseTypes.contains(messageClass))
                    msgKeyValue = messageKeyGetMethodInvoker.invokeGetter(msg);
            } catch (final IllegalArgumentException e1) {
                stopTryingToSendTheseTypes.add(msg.getClass());
                logger.warn("unable to retrieve key from message: " + String.valueOf(message) +
                        (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") +
                        "\" Please make sure its has a simple getter appropriately annotated: " +
                        e1.getLocalizedMessage()); // no stack trace.
            } catch (final IllegalAccessException e1) {
                stopTryingToSendTheseTypes.add(msg.getClass());
                logger.warn("unable to retrieve key from message: " + String.valueOf(message) +
                        (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") +
                        "\" Please make sure all annotated getter access is public: " +
                        e1.getLocalizedMessage()); // no stack trace.
            } catch (final InvocationTargetException e1) {
                logger.warn("unable to retrieve key from message: " + String.valueOf(message) +
                        (message != null ? "\" of type \"" + SafeString.valueOf(message.getClass()) : "") +
                        "\" due to an exception thrown from the getter: " +
                        e1.getLocalizedMessage(), e1.getCause());
            }

            if (msgKeyValue == null)
                logger.warn("Null message key for \"" + SafeString.valueOf(msg) +
                        (msg != null ? "\" of type \"" + SafeString.valueOf(msg.getClass()) : "") + "\"");

            else {
                ret.add(new KeyedMessage(msgKeyValue, toSend, messageClass.getName()));
            }
        }

        return null;
    }

    private List<Class<?>> getAcceptedMessages() {
        final List<Class<?>> messageClasses = new ArrayList<Class<?>>();
        if (prototype != null) {
            for (final Method method : prototype.getClass().getMethods()) {
                if (method.isAnnotationPresent(MessageHandler.class)) {
                    for (final Class<?> messageType : method.getParameterTypes()) {
                        messageClasses.add(messageType);
                    }
                }
            }
        }
        return messageClasses;
    }

    @Override
    public void start(final ClusterId clusterId) {
        for (final Method method : prototype.getClass().getMethods()) {
            if (method.isAnnotationPresent(Start.class)) {
                // if the start method takes a ClusterId or ClusterDefinition then pass it.
                final Class<?>[] parameterTypes = method.getParameterTypes();
                boolean takesClusterId = false;
                if (parameterTypes != null && parameterTypes.length == 1) {
                    if (ClusterId.class.isAssignableFrom(parameterTypes[0]))
                        takesClusterId = true;
                    else {
                        logger.error("The method \"" + method.getName() + "\" on " + SafeString.objectDescription(prototype) +
                                " is annotated with the @" + Start.class.getSimpleName() + " annotation but doesn't have the correct signature. " +
                                "It needs to either take no parameters or take a single " + ClusterId.class.getSimpleName() + " parameter.");

                        return; // return without invoking start.
                    }
                } else if (parameterTypes != null && parameterTypes.length > 1) {
                    logger.error("The method \"" + method.getName() + "\" on " + SafeString.objectDescription(prototype) +
                            " is annotated with the @" + Start.class.getSimpleName() + " annotation but doesn't have the correct signature. " +
                            "It needs to either take no parameters or take a single " + ClusterId.class.getSimpleName() + " parameter.");
                    return; // return without invoking start.
                }
                try {
                    if (takesClusterId)
                        method.invoke(prototype, clusterId);
                    else
                        method.invoke(prototype);
                    break; // Only allow one such method, which is checked during validation
                } catch (final Exception e) {
                    logger.error(MarkerFactory.getMarker("FATAL"), "can't run MP initializer " + method.getName(), e);
                }
            }
        }
    }

}
