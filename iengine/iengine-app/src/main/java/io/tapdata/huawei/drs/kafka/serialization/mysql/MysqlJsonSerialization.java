package io.tapdata.huawei.drs.kafka.serialization.mysql;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.huawei.drs.kafka.MysqlOpType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;
import io.tapdata.huawei.drs.kafka.serialization.mysql.types.*;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

/**
 * JSON 序列化实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/18 16:41 Create
 */
@Slf4j
public class MysqlJsonSerialization extends JsonSerialization {
    public MysqlJsonSerialization() {
        super(new BigintMysqlType(),
            new BinaryMysqlType(),
            new BlobMysqlType(),
            new CharMysqlType(),
            new DateMysqlType(),
            new DatetimeMysqlType(),
            new DecimalMysqlType(),
            new DoubleMysqlType(),
            new FloatMysqlType(),
            new IntMysqlType(),
            new JsonMysqlType(),
            new TextMysqlType(),
            new TimeMysqlType(),
            new TimestampMysqlType(),
            new VarbinaryMysqlType(),
            new VarcharMysqlType()
        );
    }

    @Override
    protected String getOp(JSONObject jsonValue) {
        return jsonValue.getString("type");
    }

    @Override
    protected JSONObject getFieldTypes(JSONObject jsonValue) {
        return jsonValue.getJSONObject("mysqlType");
    }

    @Override
    protected boolean decodeRecord(String tableName, Long referenceTime, JSONObject jsonValue, String opStr, Consumer<TapEvent> sender) {
        switch (MysqlOpType.fromValue(opStr)) {
            case INSERT:
            case INIT:
                return decodeInsertRecord(tableName, referenceTime, jsonValue, sender);
            case UPDATE:
                return decodeUpdateRecord(tableName, referenceTime, jsonValue, sender);
            case DELETE:
                return decodeDeleteRecord(tableName, referenceTime, jsonValue, sender);
            default:
                return false;
        }
    }
}
