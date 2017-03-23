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

package net.dempsy.transport;

/**
 * <p>
 * A transport represents a handle to both the send side and the receive side. It can be instantiated in both places and should be implemented to only create the side that's asked for.
 * </p>
 * 
 * <p>
 * Instances of the Transport are supposed to be stateless. Therefore each call on createOutbound or createInbound will freshly instantiate a new instance of the SenderFactory or Receiver
 * </p>
 */
public interface Transport {
    /**
     * Create a new instance of the Sender factory for this transport. This SenderFactory should be able to create Senders that can connect to Receivers instantiated from the getInbound call by using the
     * Destinations the Reciever generates.
     * 
     * The executor is the centralized Executor for worker threads in Dempsy. The implementor of the transport may or may not choose to use it. It MAY be null. The executor will have already been started and
     * should not be started by the transport.
     */
    public SenderFactory createOutbound() throws MessageTransportException;

    /**
     * Create a new instance of the Receiver for this transport.This Receiver should be able to create Destinations from which the SenderFactory instantiated from the getOutbound can then instantiate Senders.
     * 
     * The executor is the centralized Executor for worker threads in Dempsy. The implementor of the transport may or may not choose to use it. It MAY be null. The executor will have already been started and
     * should not be started by the transport.
     */
    public Receiver createInbound() throws MessageTransportException;

}
