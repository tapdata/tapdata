package io.tapdata.flow.engine.V2.util;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.OsUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.error.ExternalStorageExCode_26;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ExternalStorageUtilTest {

    @Test
     void getRocksDBConfigTest(){
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
    @DisplayName("testGetTapdataOrDefaultExternalStorage when ExternalStorage config is null")
     @Test
     void testGetTapdataOrDefaultExternalStorage() {
         HttpClientMongoOperator httpClientMongoOperator = mock(HttpClientMongoOperator.class);
         ConnectorConstant.clientMongoOperator=httpClientMongoOperator;
         TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
             ExternalStorageDto tapdataOrDefaultExternalStorage = ExternalStorageUtil.getTapdataOrDefaultExternalStorage();
         });
         assertEquals(ExternalStorageExCode_26.CANNOT_FOUND_EXTERNAL_STORAGE_CONFIG,tapCodeException.getCode());
     }

}
