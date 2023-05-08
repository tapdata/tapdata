package io.tapdata.connector.mysql.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author GavinXiao
 * @description MySqlPartitionBinlogPosition create by Gavin
 * @create 2023/4/18 12:49
 **/
public class MySqlPartitionBinlogPosition {
    Map<String, MysqlBinlogPosition> positionMap;

    public MySqlPartitionBinlogPosition(Map<String, MysqlBinlogPosition> positionMap) {
        this.positionMap = null == positionMap ? new HashMap<>() : positionMap;
    }
    public MySqlPartitionBinlogPosition(String partitionSetId, MysqlBinlogPosition position){
        if (this.positionMap == null){
            this.positionMap = new HashMap<>();
        }
        this.positionMap.put(partitionSetId, position);
    }

    public MysqlBinlogPosition getPosition(String partitionSetId){
        return null == this.positionMap ? null : positionMap.get(partitionSetId);
    }
}
