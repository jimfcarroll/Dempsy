package net.dempsy;

import net.dempsy.config.Cluster;
import net.dempsy.lifecycle.simple.MessageProcessor;
import net.dempsy.lifecycle.simple.Mp;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;

public class TestAck {

    public static class Acker implements Mp {

        @Override
        public KeyedMessageWithType[] handle(final KeyedMessage message) {
            return null;
        }

    }

    public static class Ack {

    }

    public void testAck() throws Exception {
        new Cluster("ack")
            .mp(new MessageProcessor(() -> new Acker()))
            .routing("net.dempsy.router.direct");

    }

}
