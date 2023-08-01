package org.apache.spark.sql.delta.catalog;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.delta.storage.DelegatingLogStore;
import org.apache.spark.sql.delta.storage.LogStoreAdaptor;
import ru.yandex.cloud.custom.delta.YcS3YdbLogStore;

/**
 *
 * @author zinal
 */
public class YcDeltaCatalog extends DeltaCatalog {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YcDeltaCatalog.class);

    @Override
    public boolean purgeTable(Identifier ident) {
        final Table table = super.loadTable(ident);
        if (table instanceof DeltaTableV2) {
            final MetadataCleanup cleanup = new MetadataCleanup(ident, table);
            if ( super.purgeTable(ident) ) {
                cleanup.run();
                return true;
            }
        } else {
            return super.purgeTable(ident);
        }
        return false;
    }

    @Override
    public boolean dropTable(Identifier ident) {
        final Table table = super.loadTable(ident);
        if (table instanceof DeltaTableV2) {
            final MetadataCleanup cleanup = new MetadataCleanup(ident, table);
            if ( super.dropTable(ident) ) {
                cleanup.run();
                return true;
            }
        } else {
            return super.dropTable(ident);
        }
        return false;
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        if (! cascade) {
            return super.dropNamespace(namespace, cascade);
        }
        for (Identifier id : listTables(namespace)) {
            if (! dropTable(id)) {
                LOG.warn("Failed to drop table {} as cascade, cannot drop namespace {}", id, namespace);
                throw new NonEmptyNamespaceException(namespace);
            }
        }
        return super.dropNamespace(namespace, cascade);
    }

    private static Object getLogStore(DeltaTableV2 v2table, Path path) {
        Object logStore = v2table.deltaLog().store();
        if (logStore instanceof DelegatingLogStore) {
            logStore = ((DelegatingLogStore) logStore).getDelegate(path);
        }
        if (logStore instanceof LogStoreAdaptor) {
            logStore = ((LogStoreAdaptor) logStore).logStoreImpl();
        }
        return logStore;
    }

    private static final class MetadataCleanup {
        final Identifier ident;
        final Object logStore;
        final Path tablePath;

        MetadataCleanup(Identifier ident, Table table) {
            Object varLogStore = null;
            Path varTablePath = null;
            if (table instanceof DeltaTableV2) {
                try {
                    final DeltaTableV2 v2table = (DeltaTableV2) table;
                    varTablePath = v2table.path();
                    varLogStore = getLogStore(v2table, varTablePath);
                } catch(Exception ex) {
                    varLogStore = null;
                    varTablePath = null;
                    LOG.warn("Failed to retrieve metadata cleanup parameters. "
                            + "LEAKING metadata for dropped table {}", ident, ex);
                }
            }
            this.ident = ident;
            this.logStore = varLogStore;
            this.tablePath = varTablePath;
        }

        void run() {
            if (ident==null || logStore==null || tablePath==null) {
                return; // Nothing to do, non-delta table.
            }
            if (! (logStore instanceof YcS3YdbLogStore)) {
                LOG.warn("Unsupported LogStore implementation {}. "
                        + "LEAKING metadata for dropped table {}", logStore.getClass(), ident);
                return;
            }
            try {
                LOG.debug("Performing metadata cleanup for table {}", tablePath);
                ((YcS3YdbLogStore) logStore).removeMetadata(tablePath.toString());
            } catch(Exception ex) {
                LOG.warn("Failed to remove metadata on {}. "
                        + "LEAKING metadata for dropped table {}",  tablePath, ident, ex);
            }
        }

    }

}
