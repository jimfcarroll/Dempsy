package net.dempsy;

import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;

public abstract class AdaptorWithAck implements Adaptor {
    public final Adaptor underlying;

    public AdaptorWithAck(final Adaptor toDecorate) {
        this.underlying = toDecorate;
    }

    @Override
    public final void setDispatcher(final Dispatcher dispatcher) {
        underlying.setDispatcher(dispatcher);
    }

    @Override
    public void start() {
        underlying.start();
    }

    @Override
    public void stop() {
        underlying.stop();
    }

}
