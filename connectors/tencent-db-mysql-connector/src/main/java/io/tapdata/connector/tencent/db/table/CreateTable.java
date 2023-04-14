package io.tapdata.connector.tencent.db.table;

import io.tapdata.connector.mysql.MysqlMaker;

/**
 * @author GavinXiao
 * @description CreateTable create by Gavin
 * @create 2023/4/14 13:39
 **/
public abstract class CreateTable extends MysqlMaker {
    public static MysqlMaker sqlMaker(String createType, String partitionKey){
        switch (createType){
            case "BroadcastTable": return new BroadcastTable();
            case "PartitionTable": return new PartitionTable().partitionKey(partitionKey);
            default: return new SingleTable();
        }
    }
}
