package net.dempsy.transport.tcp.nio;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;

import net.dempsy.util.io.MessageBufferOutput;

class NioUtils {
    // =============================================================================
    // These classes manage the buffer pool used by the readers and clients
    // =============================================================================
    static ConcurrentLinkedQueue<ReturnableBufferOutput> bufferPool = new ConcurrentLinkedQueue<>();

    static ReturnableBufferOutput get() {
        ReturnableBufferOutput ret = bufferPool.poll();
        if (ret == null)
            ret = new ReturnableBufferOutput();
        return ret;
    }

    static void closeQueitly(final AutoCloseable ac, final Logger LOGGER, final String failedMessage) {
        try {
            ac.close();
        } catch (final Exception e) {
            LOGGER.warn(failedMessage, e);
        }
    }

    static class ReturnableBufferOutput extends MessageBufferOutput {
        private ByteBuffer bb = null;
        public int messageStart = -1;
        public int numMessages = 0;

        private ReturnableBufferOutput() {
            super(2048); /// holds at least one full packet
        }

        public ByteBuffer getBb() {
            if (bb == null)
                bb = ByteBuffer.wrap(getBuffer());
            return bb;
        }

        public void flop() {
            bb = null;
            final ByteBuffer lbb = getBb();
            lbb.limit(getPosition());
        }

        @Override
        public void close() {
            super.close();
            reset();
            messageStart = -1;
            numMessages = 0;
            bb.clear();
            bufferPool.offer(this);
        }

        @Override
        public void grow(final int newcap) {
            super.grow(newcap);
            if (bb != null) {
                final ByteBuffer obb = bb;
                bb = ByteBuffer.wrap(getBuffer());
                bb.position(obb.position());
                bb.limit(obb.limit());
            }
        }
    }

}
