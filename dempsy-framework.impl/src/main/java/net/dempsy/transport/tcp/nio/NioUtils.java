package net.dempsy.transport.tcp.nio;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    static class ReturnableBufferOutput extends MessageBufferOutput {
        public ByteBuffer bb;
        public int messageStart = -1;

        private ReturnableBufferOutput() {
            super(2048); /// holds at least one full packet
            bb = ByteBuffer.wrap(getBuffer());
        }

        public void flop() {
            bb.clear();
            bb.limit(getPosition());
        }

        @Override
        public void close() {
            super.close();
            reset();
            messageStart = -1;
            bb.clear();
            bufferPool.offer(this);
        }

        @Override
        public void grow() {
            super.grow();
            final ByteBuffer obb = bb;
            bb = ByteBuffer.wrap(getBuffer());
            bb.position(obb.position());
            bb.limit(obb.limit());
        }

        @Override
        public void grow(final int newcap) {
            super.grow(newcap);
            final ByteBuffer obb = bb;
            bb = ByteBuffer.wrap(getBuffer());
            bb.position(obb.position());
            bb.limit(obb.limit());
        }
    }

}
