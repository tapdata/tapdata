package io.tapdata.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;

import java.util.LinkedHashMap;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/8 16:16 Create
 */
public class ConnectionTapTableMap implements KVReadOnlyMap<TapTable> {

    private final String connectionId;
    private final String connectionName;
    private final LinkedHashMap<String, TapTableEntry> map = new LinkedHashMap<>();

    public ConnectionTapTableMap(String connectionId, String connectionName) {
        this.connectionId = connectionId;
        this.connectionName = connectionName;
    }

    @Override
    public TapTable get(String tableName) {
        TapTableEntry ins = map.get(tableName);
        if (null == ins) {
            synchronized (map) {
                ins = map.computeIfAbsent(tableName, TapTableEntry::new);
            }
        }
        return ins.getValue();
    }

    @Override
    public Iterator<Entry<TapTable>> iterator() {
        return (Iterator<Entry<TapTable>>) map.values().iterator();
    }

    private class TapTableEntry implements Entry<TapTable> {
        private final String tableName;
        private TapTable tapTable = null;

        public TapTableEntry(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public String getKey() {
            return tableName;
        }

        @Override
        public synchronized TapTable getValue() {
            // synchronized: Limit the same table name to be loaded only once
            if (null == tapTable) {
                tapTable = TapTableUtil.getTapTableByConnectionId(connectionId, tableName);
            }
            return tapTable;
        }
    }
}
