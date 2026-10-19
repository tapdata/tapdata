package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import com.tapdata.tm.commons.util.MetaType;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamic.FunctionProxy;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.schema.TapTableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 11:13 Create
 * @description
 */
public abstract class StreamReadBaseProxy<T extends TapFunction, V> extends FunctionProxy<T> {

    protected StreamReadBaseProxy(T function) {
        super(function);
    }

    @Override
    protected boolean doBefore(Object[] args) {
        boolean hasNext = super.doBefore(args);
        TapConnectorContext context = (TapConnectorContext) args[0];
        List<V> tables = (List<V>) args[1];
        Log log = context.getLog();
        hasNext = handlerView(context, tables, log, hasNext);
        return hasNext;
    }

    protected boolean handlerView(TapConnectorContext context, List<V> tables, Log log, boolean hasNext) {
        KVReadOnlyMap<TapTable> tableMap = context.getTableMap();
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        Set<String> view = new HashSet<>();
        while (iterator.hasNext()) {
            Entry<TapTable> entry = iterator.next();
            String tableName = entry.getKey();
            if (skip(tables, tableName)) {
                continue;
            }
            TapTable tapTable = entry.getValue();
            String type = tapTable.getType();
            if (MetaType.isView(type)) {
                remove(tables, tableName);
                view.add(tableName);
            }
        }
        if (!view.isEmpty()) {
            log.warn("The view does not support CDC mode. The following views will skip the CDC phase: {}", view.stream().sorted().collect(Collectors.joining(", ")));
        }
        if (tables.isEmpty() && !view.isEmpty()) {
            return false;
        }
        return hasNext;
    }

    public static void judgeTable(TapTableMap<String, TapTable> tableMap, ObsLogger log) {
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        Set<String> view = new HashSet<>();
        while (iterator.hasNext()) {
            Entry<TapTable> entry = iterator.next();
            String tableName = entry.getKey();
            TapTable tapTable = entry.getValue();
            String type = tapTable.getType();
            if (MetaType.isView(type)) {
                tableMap.remove(tableName);
                view.add(tableName);
            }
        }
        if (!view.isEmpty()) {
            log.warn("The view does not support CDC mode. The following views will skip the CDC phase: {}", view.stream().sorted().collect(Collectors.joining(", ")));
        }
    }

    protected abstract void remove(List<V> tables, String tableName);

    protected boolean skip(List<V> tables, String tableName) {
        return false;
    }
}
