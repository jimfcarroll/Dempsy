package net.dempsy.router.microshard;

import java.util.Collection;

import net.dempsy.DempsyException;
import net.dempsy.cluster.ClusterInfoException;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoWatcher;
import net.dempsy.cluster.DirMode;

public class Transaction implements ClusterInfoWatcher {
    private static final String txPrefix = "Tx_";

    private final String transactionDirPath;
    private final String transactionPath;
    private final ClusterInfoSession session;
    private final ClusterInfoWatcher proxied;
    private String actualPath = null;

    public Transaction(final String transactionDirPath, final ClusterInfoSession session, final ClusterInfoWatcher proxied) {
        this.transactionDirPath = transactionDirPath;
        this.transactionPath = transactionDirPath + "/" + txPrefix;
        this.session = session;
        this.proxied = proxied;
    }

    public void open() throws ClusterInfoException {
        if (actualPath != null) {
            if (session.exists(actualPath, null))
                return;
            else
                actualPath = null;
        }

        final String tmp = session.mkdir(transactionPath, null, DirMode.EPHEMERAL_SEQUENTIAL);

        final Collection<String> subdirs = session.getSubdirs(transactionDirPath, this);

        // double check that we're there....
        final String baseName = tmp.substring(tmp.lastIndexOf('/'));
        if (!subdirs.contains(baseName))
            throw new ClusterInfoException("Inconsistent transaction. Created " + tmp + " but it's missing from " + subdirs);

        actualPath = tmp; // don't set the actualPath until we're registered.
    }

    public void close() throws ClusterInfoException {
        if (actualPath == null) // we're not open ... but we're going to open and close.
            open();

        // now close.
        session.rmdir(transactionPath); // remove the tx marker.
    }

    @Override
    public void process() {
        try {
            final Collection<String> subdirs = session.getSubdirs(transactionDirPath, this);
            if (subdirs.size() == 0)
                proxied.process();
        } catch (final ClusterInfoException e) {
            throw new DempsyException("Failed to check transaction state.", e);
        }
    }
}
