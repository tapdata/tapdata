package io.tapdata.flow.engine.V2.util;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.tapdata.constant.OsUtil;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    @DisplayName("test get mongoUri without additionalString")
    @Test
    void test1() {
        Map<String, Object> config = new HashMap<>();
        config.put("user", "testUser");
        config.put("password", "testPassword");
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        String mongoUri = ExternalStorageUtil.getMongoUri(config);
        assertEquals("mongodb://testUser:testPassword@localhost/testDatabase", mongoUri);
    }

     @DisplayName("test get mongoUri with additionalString")
     @Test
     void test2() {
         Map<String, Object> config = new HashMap<>();
         config.put("user", "testUser");
         config.put("password", "testPassword");
         config.put("host", "localhost");
         config.put("database", "testDatabase");
         config.put("additionalString", "?authSource=admin");
         String mongoUri = ExternalStorageUtil.getMongoUri(config);
         assertEquals("mongodb://testUser:testPassword@localhost/testDatabase?authSource=admin", mongoUri);
     }
     @DisplayName("test get mongoUri with user and password")
     @Test
     void test3(){
         Map<String, Object> config = new HashMap<>();
         config.put("host", "localhost");
         config.put("database", "testDatabase");
         config.put("additionalString", "?authSource=admin");
         String mongoUri = ExternalStorageUtil.getMongoUri(config);
         assertEquals("mongodb://localhost/testDatabase?authSource=admin", mongoUri);
     }

}
