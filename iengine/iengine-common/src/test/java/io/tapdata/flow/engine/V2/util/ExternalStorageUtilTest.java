package io.tapdata.flow.engine.V2.util;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.tapdata.constant.OsUtil;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExternalStorageUtilTest {

    @Test
    public void getRocksDBConfigTest(){
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        String rocksdbPath = "/data/test/tt";
        externalStorageDto.setUri(rocksdbPath);
        ExternalStorageUtil externalStorageUtil = new ExternalStorageUtil();
        PersistenceRocksDBConfig actualData = ReflectionTestUtils.invokeMethod(externalStorageUtil, "getRocksDBConfig",
                externalStorageDto,ConstructType.RINGBUFFER,"test" );


        String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
        if (com.tapdata.manager.common.utils.StringUtils.isBlank(tapdataWorkDir)) {
            tapdataWorkDir = System.getProperty("user.dir");
        }
        rocksdbPath = tapdataWorkDir + rocksdbPath;
        if (OsUtil.isWindows()) {
            rocksdbPath = rocksdbPath.replace("/","\\");
        }

        String exceptData = rocksdbPath;
        assertEquals(exceptData,actualData.getPath());

    }
}
