package io.tapdata.huawei.drs.kafka.serialization;

import com.tapdata.huawei.drs.kafka.FromDBType;
import io.tapdata.huawei.drs.kafka.ISerialization;
import io.tapdata.huawei.drs.kafka.serialization.mysql.MysqlJsonSerialization;
import io.tapdata.huawei.drs.kafka.serialization.oracle.OracleJsonSerialization;

/**
 * JSON 序列化实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 16:41 Create
 */
public abstract class JsonSerialization implements ISerialization {

    public static JsonSerialization create(String fromDBType) {
        FromDBType fromDBTypeEnum = FromDBType.fromValue(fromDBType);
        switch (fromDBTypeEnum) {
            case MYSQL:
            case GAUSSDB_MYSQL:
                return new MysqlJsonSerialization();
            case ORACLE:
            case MSSQL:
            case POSTGRESQL:
            case GAUSSDB:
                return new OracleJsonSerialization();
            default:
                throw new RuntimeException("un-support DB type '" + fromDBType + "'");
        }
    }
}
