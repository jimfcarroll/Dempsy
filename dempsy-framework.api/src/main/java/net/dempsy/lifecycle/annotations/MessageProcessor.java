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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

import net.dempsy.config.ClusterDefinition;
import net.dempsy.config.ClusterId;
import net.dempsy.lifecycle.annotations.internal.AnnotatedMethodInvoker;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.KeySource;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.util.SafeString;

/**
 * This class holds the MP prototype, and supports invocation of MP methods on an instance.
 */
public class MessageProcessor implements MessageProcessorLifecycle {
    private final Object prototype;
    private final Class<?> mpClass;
    private final String mpClassName;

    private final String toStringValue;

    private final Method cloneMethod;
    private MethodHandle activationMethod;
    private final MethodHandle passivationMethod;
    private final Method outputMethod;
    private final MethodHandle evictableMethod;
    private final AnnotatedMethodInvoker invocationMethods;

    public MessageProcessor(final Object prototype) {
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
        outputMethod = AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Output.class);
        evictableMethod = new MethodHandle(AnnotatedMethodInvoker.introspectAnnotationSingle(mpClass, Evictable.class));
    }

    public static class CdBuild {
        private final ClusterDefinition cd;

        public CdBuild(final String clusterName) {
            cd = new ClusterDefinition(clusterName);
        }

        public CdBuild(final String clusterName, final Object mp) {
            this(clusterName);
            prototype(mp);
        }

        public CdBuild(final String clusterName, final Adaptor mp) {
            this(clusterName);
            adaptor(mp);
        }

        public CdBuild prototype(final Object mp) {
            cd.setMessageProcessor(new MessageProcessor(mp));
            return this;
        }

        public CdBuild adaptor(final Adaptor adaptor) {
            cd.setAdaptor(adaptor);
            return this;
        }

        public CdBuild downstream(final String... downstreamClusterNames) {
            Arrays.stream(downstreamClusterNames).forEach(cn -> cd.setDestinations(new ClusterId(null, cn)));
            return this;
        }

        public CdBuild statsCollector(final Object statsCollector) {
            cd.setStatsCollectorFactory(statsCollector);
            return this;
        }

        public CdBuild keySource(final KeySource<?> keySource) {
            cd.setKeySource(keySource);
            return this;
        }

        public ClusterDefinition cd() {
            return cd;
        }
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
    public boolean activate(final Object instance, final Object key, final byte[] activationData)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        final Object o = activationMethod.invoke(instance, key, activationData);
        return o == null ? true : ((Boolean) o);
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
    public byte[] passivate(final Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
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
    public Object invoke(final Object instance, final Object message)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        final Class<?> messageClass = message.getClass();
        if (!isMessageSupported(message))
            throw new IllegalArgumentException(mpClassName + ": no handler for messages of type: " + messageClass.getName());

        return invocationMethods.invokeMethod(instance, message);
    }

    @Override
    public String invokeDescription(final Operation op, final Object message) {
        Method method = null;
        Class<?> messageClass = void.class;
        if (op == Operation.output)
            method = outputMethod;
        else {
            if (message == null)
                return toStringValue + ".?(null)";

            messageClass = message.getClass();
            if (!isMessageSupported(message))
                return toStringValue + ".?(" + messageClass.getName() + ")";

            method = invocationMethods.getMethodForClass(message.getClass());
        }

        return toStringValue + "." + (method == null ? "?" : method.getName()) + "(" + messageClass.getSimpleName() + ")";
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
    public Object invokeOutput(final Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        return (outputMethod != null) ? outputMethod.invoke(instance) : null;
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
    public boolean invokeEvictable(final Object instance) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        return isEvictableSupported() ? (Boolean) evictableMethod.invoke(instance) : false;
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

    /**
     * Determines whether this MP provides an output method.
     */
    @Override
    public boolean isOutputSupported() {
        return outputMethod != null;
    }

    /**
     * Determines whether this MP provides an evictable method.
     */
    @Override
    public boolean isEvictableSupported() {
        return evictableMethod.getMethod() != null;
    }

    public void validate() throws IllegalStateException {
        if (prototype != null) {
            if (!prototype.getClass().isAnnotationPresent(Mp.class))
                throw new IllegalStateException("Attempting to set an instance of \"" +
                        SafeString.valueOfClass(prototype) + "\" within the " +
                        ClusterDefinition.class.getSimpleName() +
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
            final MessageProcessor that = (MessageProcessor) obj;
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

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    private void validateAsMP() throws IllegalStateException {
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

}
