package io.tapdata.huawei.drs.kafka.serialization.oracle;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.huawei.drs.kafka.OracleOpType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;
import io.tapdata.huawei.drs.kafka.serialization.oracle.types.*;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 16:25 Create
 */
@Slf4j
public class OracleJsonSerialization extends JsonSerialization {

    public OracleJsonSerialization() {
        super(new BitOracleType(),
            new BooleanOracleType(),
            new CharacterOracleType(),
            new JsonOracleType(),
            new SmallintMysqlType(),
            new TimestampWithoutTimeZoneMysqlType(),
            new VarcharOracleType()
        );
    }

    @Override
    protected String getOp(JSONObject jsonValue) {
        return jsonValue.getString("opType");
    }

    @Override
    protected JSONObject getFieldTypes(JSONObject jsonValue) {
        return jsonValue.getJSONObject("columnType");
    }

    @Override
    protected boolean decodeRecord(String tableName, Long referenceTime, JSONObject jsonValue, String opStr, Consumer<TapEvent> sender) {
        switch (OracleOpType.fromValue(opStr)) {
            case INSERT:
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
