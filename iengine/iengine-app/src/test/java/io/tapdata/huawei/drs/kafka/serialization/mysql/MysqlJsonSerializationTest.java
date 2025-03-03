package io.tapdata.huawei.drs.kafka.serialization.mysql;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.huawei.drs.kafka.MysqlOpType;
import com.tapdata.huawei.drs.kafka.OracleOpType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 18:26 Create
 */
class MysqlJsonSerializationTest {

    MysqlJsonSerialization ins;
    MysqlJsonSerialization mockIns;

    @BeforeEach
    void setUp() {
        ins = new MysqlJsonSerialization(false);
        mockIns = Mockito.spy(ins);
    }

    protected JSONObject mockJsonValue(OracleOpType type) {
        return JSONObject.parseObject("{'type':'" + type.name() + "', 'mysqlType':{}, 'data':[], 'old':[]}");
    }

    protected void checkWithType(OracleOpType type) {
        JSONObject jsonValue = mockJsonValue(type);
        String op = ins.getOp(jsonValue);
        Assertions.assertTrue(mockIns.decodeRecord(null, null, jsonValue, op, null));
    }

    @Nested
    class DecodeRecordTest {
        @Test
        void testUnSupport() {
            String opStr = MysqlOpType.UNDEFINED.name();
            Assertions.assertFalse(ins.decodeRecord(null, null, null, opStr, null));
        }

        @Test
        void testInsert() {
            checkWithType(OracleOpType.INSERT);
        }

        @Test
        void testDelete() {
            checkWithType(OracleOpType.DELETE);
        }

        @Test
        void testUpdate() {
            checkWithType(OracleOpType.UPDATE);
        }
    }
}
