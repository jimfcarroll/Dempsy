package net.dempsy.router.direct;

public abstract class Key {

    public final String destinationGuid;

    public Key(final String destinationGuid) {
        this.destinationGuid = destinationGuid;
    }
}
