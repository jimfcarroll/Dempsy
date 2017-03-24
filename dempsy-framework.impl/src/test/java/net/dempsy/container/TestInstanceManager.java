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

package net.dempsy.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import net.dempsy.config.ClusterId;
import net.dempsy.container.Container.InstanceWrapper;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageProcessor;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.Output;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.monitoring.StatsCollector;

public class TestInstanceManager {

    private Container manager;

    // ----------------------------------------------------------------------------
    // Test classes -- must be static/public for introspection
    // ----------------------------------------------------------------------------

    @MessageType
    public static class MessageOne {
        private final Integer keyValue;

        public MessageOne(final int keyValue) {
            this.keyValue = Integer.valueOf(keyValue);
        }

        @MessageKey
        public Integer getKey() {
            return keyValue;
        }
    }

    public static class MessageTwo {
        private final Integer keyValue;

        public MessageTwo(final int keyValue) {
            this.keyValue = Integer.valueOf(keyValue);
        }

        @MessageKey
        public Integer getKey() {
            return keyValue;
        }
    }

    @MessageType
    public static class ReturnString {
        public final String value;

        public ReturnString(final String value) {
            this.value = value;
        }

        @MessageKey
        public String getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value.equals(((ReturnString) o).value);
        }

    }

    @Mp
    public static class CombinedMP
            implements Cloneable {
        public long activationCount;
        public long activationTime;

        public long firstMessageTime = -1;
        public List<Object> messages; // note: can't be shared

        @Override
        public CombinedMP clone()
                throws CloneNotSupportedException {
            return (CombinedMP) super.clone();
        }

        @Activation
        public void activate(final byte[] data) {
            activationCount++;
            activationTime = System.nanoTime();

            messages = new ArrayList<Object>();
        }

        @MessageHandler
        public ReturnString handle(final MessageOne message) {
            if (firstMessageTime < 0)
                firstMessageTime = System.nanoTime();
            messages.add(message);
            return new ReturnString("MessageOne");
        }

        @MessageHandler
        public ReturnString handle(final MessageTwo message) {
            if (firstMessageTime < 0)
                firstMessageTime = System.nanoTime();
            messages.add(message);
            return new ReturnString("MessageTwo");
        }
    }

    @MessageType
    public static class ReturnInt {
        public final int value;

        public ReturnInt(final int value) {
            this.value = value;
        }

        @MessageKey
        public int getKey() {
            return value;
        }

        @Override
        public boolean equals(final Object o) {
            return value == ((ReturnInt) o).value;
        }

    }

    @Mp
    public static class OutputTestMP extends CombinedMP {
        public long outputTime;

        @Override
        public OutputTestMP clone() throws CloneNotSupportedException {
            return (OutputTestMP) super.clone();
        }

        @Output
        public ReturnInt doOutput() {
            outputTime = System.nanoTime();
            return new ReturnInt(42);
        }
    }

    @Mp
    public static class UnsuportedMessageTestMP implements Cloneable {
        @Override
        public UnsuportedMessageTestMP clone()
                throws CloneNotSupportedException {
            return (UnsuportedMessageTestMP) super.clone();
        }

        @MessageHandler
        public void handle(final MessageOne message) {
            // this method will never get called
        }
    }

    public static class MessageWithNullKey {
        @MessageKey
        public Integer getKey() {
            return null;
        }
    }

    @Mp
    public static class NullKeyTestMP
            implements Cloneable {
        @Override
        public NullKeyTestMP clone()
                throws CloneNotSupportedException {
            return (NullKeyTestMP) super.clone();
        }

        @MessageHandler
        public void handle(final MessageWithNullKey message) {
            // this method will never get called
        }
    }

    public static class DummyDispatcher implements Dispatcher {
        public KeyedMessage lastDispatched;
        public KeyExtractor ke;

        @Override
        public void dispatch(final KeyedMessage message) {
            this.lastDispatched = message;
        }

        @Override
        public void dispatchAnnotated(final Object message) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            dispatch(ke.extract(message));
        }
    }

    private static KeyedMessage km(final Object message) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return new KeyExtractor().extract(message).get(0);
    }

    // ----------------------------------------------------------------------------
    // Test Cases
    // ----------------------------------------------------------------------------

    public Container setupContainer(final MessageProcessorLifecycle prototype) throws ContainerException {
        final DummyDispatcher dispatcher = new DummyDispatcher();
        final StatsCollector stats = new BasicStatsCollector();

        manager = new Container(prototype, new ClusterId("test", "test"));
        manager.setDispatcher(dispatcher);
        manager.setStatCollector(stats);
        return manager;
    }

    @Test
    public void testSingleInstanceOneMessage() throws Throwable {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));

        assertEquals("starts with no instances", 0, manager.getProcessorCount());

        final KeyedMessage message = km(new MessageOne(123));
        final InstanceWrapper wrapper = manager.getInstanceForDispatch(message);
        assertEquals("instance was created", 1, manager.getProcessorCount());

        final CombinedMP instance = (CombinedMP) wrapper.getInstance();
        // activation is now inline with insantiation so it's active immediately
        // assertEquals("instance not already activated", 0, instance.activationCount);
        assertEquals("instance activated", 1, instance.activationCount);
        assertEquals("instance has no existing messages", -1, instance.firstMessageTime);
        // assertNull("instance has no message list", instance.messages);
        assertTrue("real activation time", instance.activationTime > 0);
        assertEquals("message count", 0, instance.messages.size());

        // dispatch the message
        // wrapper.run();
        manager.dispatch(message, true);
        assertEquals("instance activated", 1, instance.activationCount);
        assertTrue("real activation time", instance.activationTime > 0);
        assertSame("instance received message", message.message, instance.messages.get(0));
        assertEquals("message count", 1, instance.messages.size());
        assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
        // The return value cannot be routed.
        assertEquals(new ReturnString("MessageOne"), ((DummyDispatcher) manager.getDispatcher()).lastDispatched.message);

        assertEquals("prototype not activated", 0, prototype.activationCount);
        assertEquals("prototype did not receive messages", -1, prototype.firstMessageTime);
        assertNull("prototype has no message list", prototype.messages);
    }

    @Test
    public void testSingleInstanceTwoMessagesSameClassSeparateExecution()
            throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        assertEquals("starts with no instances", 0, manager.getProcessorCount());

        final KeyedMessage message1 = km(new MessageOne(123));
        final InstanceWrapper wrapper1 = manager.getInstanceForDispatch(message1);
        manager.dispatch(message1, false);
        final CombinedMP instance = (CombinedMP) wrapper1.getInstance();

        assertEquals("instance was created", 1, manager.getProcessorCount());

        assertEquals("instance activated", 1, instance.activationCount);
        assertTrue("real activation time", instance.activationTime > 0);
        assertSame("instance received message", message1.message, instance.messages.get(0));
        assertEquals("message count", 1, instance.messages.size());
        assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        final KeyedMessage message2 = km(new MessageOne(123));
        final InstanceWrapper wrapper2 = manager.getInstanceForDispatch(message2);
        manager.dispatch(message2, false);
        assertSame("same wrapper returned for second message", wrapper1, wrapper2);
        assertEquals("no other instance was created", 1, manager.getProcessorCount());

        assertEquals("no second activation", 1, instance.activationCount);
        assertEquals("both messages delivered", 2, instance.messages.size());
        assertSame("message1 delivered first", message1.message, instance.messages.get(0));
        assertSame("message2 delivered second", message2.message, instance.messages.get(1));
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);
    }

    @Test
    public void testSingleInstanceTwoMessagesSameClassCombinedExecution()
            throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        assertEquals("starts with no instances", 0, manager.getProcessorCount());

        final KeyedMessage message1 = km(new MessageOne(123));
        final InstanceWrapper wrapper = manager.getInstanceForDispatch(message1);
        manager.dispatch(message1, false);
        assertEquals("instance was created", 1, manager.getProcessorCount());
        final KeyedMessage message2 = km(new MessageOne(123));
        assertSame("same wrapper returned for second message",
                wrapper, manager.getInstanceForDispatch(message2));
        manager.dispatch(message2, false);

        final CombinedMP instance = (CombinedMP) wrapper.getInstance();
        assertEquals("no other instance was created", 1, manager.getProcessorCount());

        assertEquals("instance activated", 1, instance.activationCount);
        assertTrue("real activation time", instance.activationTime > 0);
        assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
        assertEquals("both messages delivered", 2, instance.messages.size());
        assertSame("message1 delivered first", message1.message, instance.messages.get(0));
        assertSame("message2 delivered second", message2.message, instance.messages.get(1));
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);
    }

    @Test
    public void testSingleInstanceTwoMessagesDifferentClassSeparateExecution()
            throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        assertEquals("starts with no instances", 0, manager.getProcessorCount());

        final KeyedMessage message1 = km(new MessageOne(123));
        final InstanceWrapper wrapper = manager.getInstanceForDispatch(message1);
        manager.dispatch(message1, true);
        final CombinedMP instance = (CombinedMP) wrapper.getInstance();

        assertEquals("instance was created", 1, manager.getProcessorCount());

        assertEquals("instance activated", 1, instance.activationCount);
        assertTrue("real activation time", instance.activationTime > 0);
        assertSame("instance received message", message1.message, instance.messages.get(0));
        assertEquals("message count", 1, instance.messages.size());
        assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        final KeyedMessage message2 = km(new MessageTwo(123));
        assertSame("same wrapper returned for second message", wrapper, manager.getInstanceForDispatch(message2));
        manager.dispatch(message2, false);
        assertEquals("no other instance was created", 1, manager.getProcessorCount());

        assertEquals("no second activation", 1, instance.activationCount);
        assertEquals("both messages delivered", 2, instance.messages.size());
        assertSame("message1 delivered first", message1.message, instance.messages.get(0));
        assertSame("message2 delivered second", message2.message, instance.messages.get(1));
        assertEquals(new ReturnString("MessageTwo"), dispatcher.lastDispatched.message);
    }

    @Test
    public void testMultipleInstanceCreation() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        assertEquals("starts with no instances", 0, manager.getProcessorCount());

        final KeyedMessage message1 = km(new MessageOne(123));
        final InstanceWrapper wrapper1 = manager.getInstanceForDispatch(message1);
        manager.dispatch(message1, true);
        final CombinedMP instance1 = (CombinedMP) wrapper1.getInstance();

        final KeyedMessage message2 = km(new MessageOne(456));
        final InstanceWrapper wrapper2 = manager.getInstanceForDispatch(message2);
        manager.dispatch(message2, false);
        final CombinedMP instance2 = (CombinedMP) wrapper2.getInstance();

        assertEquals("instances were created", 2, manager.getProcessorCount());

        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        assertEquals("message count to instance1", 1, instance1.messages.size());
        assertEquals("message count to instance2", 1, instance2.messages.size());

        assertSame("message1 went to instance1", message1.message, instance1.messages.get(0));
        assertSame("message2 went to instance2", message2.message, instance2.messages.get(0));
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);
    }

    @Test
    public void testOutput() throws Exception {
        final OutputTestMP prototype = new OutputTestMP();
        final Container manager = setupContainer(new MessageProcessor<OutputTestMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        // we need to dispatch messages to create MP instances
        final KeyedMessage message1 = km(new MessageOne(1));
        final InstanceWrapper wrapper1 = manager.getInstanceForDispatch(message1);
        manager.dispatch(message1, true);
        final KeyedMessage message2 = km(new MessageOne(2));
        final InstanceWrapper wrapper2 = manager.getInstanceForDispatch(message2);
        manager.dispatch(message2, true);
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        manager.outputPass();

        final OutputTestMP mp1 = (OutputTestMP) wrapper1.getInstance();
        assertTrue("MP1 output did not occur after activation", mp1.activationTime < mp1.outputTime);

        final OutputTestMP mp2 = (OutputTestMP) wrapper2.getInstance();
        assertTrue("MP2 output did not occur after activation", mp2.activationTime < mp2.outputTime);
        assertTrue(mp1 != mp2);

        assertEquals(new ReturnInt(42), dispatcher.lastDispatched.message);
    }

    @Test
    public void testOutputCountsOkay() throws Exception {
        final OutputTestMP prototype = new OutputTestMP();
        final Container manager = setupContainer(new MessageProcessor<OutputTestMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        // we need to dispatch messages to create MP instances
        final KeyedMessage message1 = km(new MessageOne(1));
        final KeyedMessage message2 = km(new MessageOne(2));
        manager.dispatch(message1, true);
        manager.dispatch(message2, false);
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        manager.outputPass();
        assertEquals("number of processed messages should include outputs.", 4,
                ((MetricGetters) manager.getStatsCollector()).getProcessedMessageCount());
    }

    @Test
    public void testOutputShortCircuitsIfNoOutputMethod() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));
        final DummyDispatcher dispatcher = ((DummyDispatcher) manager.getDispatcher());

        // we need to dispatch messages to create MP instances
        final KeyedMessage message1 = km(new MessageOne(1));
        final KeyedMessage message2 = km(new MessageOne(2));
        manager.dispatch(message1, true);
        manager.dispatch(message2, false);
        assertEquals(new ReturnString("MessageOne"), dispatcher.lastDispatched.message);

        manager.outputPass();
        // output messages are NOT considered "processed" if there is no output method on the MP.
        assertEquals("number of processed messages should include outputs.", 2,
                ((MetricGetters) manager.getStatsCollector()).getProcessedMessageCount());
    }

    // This test no longer really matters since there is no queue but we might as well leave it
    // since it exercises the container.
    @Test
    public void testQueueIsClearedAfterExecution() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));

        final KeyedMessage message = km(new MessageOne(123));
        final InstanceWrapper wrapper = manager.getInstanceForDispatch(message);
        manager.dispatch(message, false);
        assertEquals("instance was created", 1, manager.getProcessorCount());

        final CombinedMP instance = (CombinedMP) wrapper.getInstance();

        assertEquals("instance activated", 1, instance.activationCount);
        assertTrue("real activation time", instance.activationTime > 0);
        assertSame("instance received message", message.message, instance.messages.get(0));
        assertEquals("message count", 1, instance.messages.size());
        assertTrue("activated before first message", instance.activationTime < instance.firstMessageTime);

        final long activationTime = instance.activationTime;
        final long firstMessageTime = instance.firstMessageTime;

        // here is where the queue would have been advanced again ... but there is no queue anymore.
        assertTrue("activation time didn't change", activationTime == instance.activationTime);
        assertTrue("message time didn't change", firstMessageTime == instance.firstMessageTime);
        assertEquals("message count didn't change", 1, instance.messages.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailureNullMessage() throws Exception {
        final CombinedMP prototype = new CombinedMP();
        final Container manager = setupContainer(new MessageProcessor<CombinedMP>(prototype));

        manager.getInstanceForDispatch(null);
    }

    @Test(expected = ContainerException.class)
    public void testFailureUnsupportedMessage() throws Exception {
        final Container dispatcher = setupContainer(new MessageProcessor<UnsuportedMessageTestMP>(new UnsuportedMessageTestMP()));
        dispatcher.getInstanceForDispatch(km(new MessageTwo(123)));
    }

    @Test(expected = ContainerException.class)
    public void testFailureNoKeyMethod() throws Exception {
        final MpContainer dispatcher = setupContainer(new NullKeyTestMP());
        dispatcher.getInstanceForDispatch(new MessageWithNullKey());
    }

    @MessageProcessor
    public static class ThrowMe implements Cloneable {
        @MessageHandler
        public void handle(final MessageOne message) {
            throw new RuntimeException("YO!");
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

    }

    @Test
    public void testMpThrows() throws Exception {
        final MpContainer dispatcher = setupContainer(new ThrowMe());

        dispatcher.dispatch(new MessageOne(123), true);

        assertEquals(1, ((MetricGetters) dispatcher.getStatsCollector()).getMessageFailedCount());
    }
}
