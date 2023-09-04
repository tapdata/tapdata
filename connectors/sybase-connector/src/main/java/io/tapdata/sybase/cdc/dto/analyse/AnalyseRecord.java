package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;

import java.util.LinkedHashMap;

/**
 * @author GavinXiao
 * @description AnalyseRecord create by Gavin
 * @create 2023/7/14 9:59
 **/
public interface AnalyseRecord<V, T> {
//    public T analyse(V record, LinkedHashMap<String, TableTypeEntity> tapTable, String tableId, ConnectionConfig config, NodeConfig nodeConfig);
    public T analyse(V record, LinkedHashMap<String, TableTypeEntity> tapTable, String tableId, ConnectionConfig config, NodeConfig nodeConfig, CsvAnalyseFilter filter);
}
