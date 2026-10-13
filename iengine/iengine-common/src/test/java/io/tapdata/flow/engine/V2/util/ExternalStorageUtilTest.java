package io.tapdata.flow.engine.V2.util;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.OsUtil;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.error.ExternalStorageExCode_26;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.data.mongodb.core.query.Criteria.where;

class ExternalStorageUtilTest {

    @Test
    void getExternalStorageForHazelCastImdgNodeSkipsMissingFirstConnection() {
        LogCollectorNode logCollectorNode = new LogCollectorNode();
        logCollectorNode.setConnectionIds(List.of("missing", "existing"));
        HazelCastImdgNode hazelCastImdgNode = new HazelCastImdgNode();

        ExternalStorageDto sharedStorage = new ExternalStorageDto();
        sharedStorage.setId(new org.bson.types.ObjectId());
        sharedStorage.setName("shared-storage");
        ExternalStorageDto defaultStorage = new ExternalStorageDto();
        defaultStorage.setId(new org.bson.types.ObjectId());
        defaultStorage.setName("default-storage");
        defaultStorage.setDefaultStorage(true);
        Map<String, ExternalStorageDto> externalStorageDtoMap = new HashMap<>();
        externalStorageDtoMap.put(sharedStorage.getId().toHexString(), sharedStorage);
        externalStorageDtoMap.put(defaultStorage.getId().toHexString(), defaultStorage);

        Connections existingConnection = new Connections();
        existingConnection.setShareCDCExternalStorageId(sharedStorage.getId().toHexString());
        TestMongoOperator mongoOperator = new TestMongoOperator();
        mongoOperator.setConnectionResults(null, existingConnection);

        assertSame(sharedStorage, ExternalStorageUtil.getExternalStorage(
                externalStorageDtoMap,
                hazelCastImdgNode,
                mongoOperator,
                List.of(logCollectorNode),
                null
        ));
    }

    @Test
    void getExternalStorageFromMongoForHazelCastImdgNodeSkipsMissingFirstConnection() {
        LogCollectorNode logCollectorNode = new LogCollectorNode();
        logCollectorNode.setConnectionIds(List.of("missing", "existing"));
        HazelCastImdgNode hazelCastImdgNode = new HazelCastImdgNode();

        ExternalStorageDto sharedStorage = new ExternalStorageDto();
        sharedStorage.setId(new org.bson.types.ObjectId());
        sharedStorage.setName("shared-storage");
        ExternalStorageDto defaultStorage = new ExternalStorageDto();
        defaultStorage.setId(new org.bson.types.ObjectId());
        defaultStorage.setName("default-storage");
        defaultStorage.setDefaultStorage(true);

        Connections existingConnection = new Connections();
        existingConnection.setShareCDCExternalStorageId(sharedStorage.getId().toHexString());
        TestMongoOperator mongoOperator = new TestMongoOperator();
        mongoOperator.setConnectionResults(null, existingConnection);
        mongoOperator.setExternalStorages(sharedStorage, defaultStorage);

        assertSame(sharedStorage, ExternalStorageUtil.getExternalStorage(
                hazelCastImdgNode,
                List.of(logCollectorNode),
                mongoOperator,
                null
        ));
    }

    private static class TestMongoOperator extends ClientMongoOperator {
        private List<Connections> connectionResults = List.of();
        private int connectionIndex;
        private ExternalStorageDto sharedStorage;
        private ExternalStorageDto defaultStorage;

        void setConnectionResults(Connections... connectionResults) {
            this.connectionResults = Arrays.asList(connectionResults);
        }

        void setExternalStorages(ExternalStorageDto sharedStorage, ExternalStorageDto defaultStorage) {
            this.sharedStorage = sharedStorage;
            this.defaultStorage = defaultStorage;
        }

        @Override
        public <T> T findOne(Query query, String collection, Class<T> className) {
            if (ConnectorConstant.CONNECTION_COLLECTION.equals(collection)) {
                return className.cast(connectionResults.get(connectionIndex++));
            }
            if (ConnectorConstant.EXTERNAL_STORAGE_COLLECTION.equals(collection)) {
                String externalStorageId = query.getQueryObject().getString("_id");
                if (sharedStorage.getId().toHexString().equals(externalStorageId)) {
                    return className.cast(sharedStorage);
                }
                return className.cast(defaultStorage);
            }
            return null;
        }
    }

    @Test
    void getExternalStorageMapSkipsMissingFirstLogCollectorConnection() {
        HttpClientMongoOperator mongoOperator = mock(HttpClientMongoOperator.class);
        TaskDto taskDto = new TaskDto();
        taskDto.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        LogCollectorNode logCollectorNode = new LogCollectorNode();
        logCollectorNode.setConnectionIds(List.of("missing", "existing"));
        DAG dag = mock(DAG.class);
        when(dag.getNodes()).thenReturn(List.of(logCollectorNode));
        taskDto.setDag(dag);

        Connections existingConnection = new Connections();
        existingConnection.setShareCDCExternalStorageId("storage-id");
        when(mongoOperator.findOne(any(Query.class), eq(ConnectorConstant.CONNECTION_COLLECTION), eq(Connections.class)))
                .thenReturn(null, existingConnection);

        ExternalStorageDto storage = new ExternalStorageDto();
        storage.setId(new org.bson.types.ObjectId());
        storage.setName("shared-storage");
        when(mongoOperator.find(any(Query.class), eq(ConnectorConstant.EXTERNAL_STORAGE_COLLECTION), eq(ExternalStorageDto.class)))
                .thenReturn(List.of(storage));

        assertEquals(storage, ExternalStorageUtil.getExternalStorageMap(taskDto, mongoOperator)
                .get(storage.getId().toHexString()));
    }

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

    @DisplayName("testGetTapdataOrDefaultExternalStorage when ExternalStorage is default")
    @Test
    void testGetDefaultExternalStorage() {
        HttpClientMongoOperator httpClientMongoOperator = mock(HttpClientMongoOperator.class);
        ConnectorConstant.clientMongoOperator=httpClientMongoOperator;
        Query query = Query.query(new Criteria().orOperator(
                where("name").is(ConnectorConstant.TAPDATA_MONGO_DB_EXTERNAL_STORAGE_NAME),
                where("defaultStorage").is(true)
        ));
        List<ExternalStorageDto> externalStorages = new ArrayList<>();
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setName("Tapdata MongoDB External Storage");
        externalStorageDto.setDefaultStorage(false);
        ExternalStorageDto defaultExternalStorageDto = new ExternalStorageDto();
        defaultExternalStorageDto.setName("test");
        defaultExternalStorageDto.setDefaultStorage(true);
        externalStorages.add(externalStorageDto);
        externalStorages.add(defaultExternalStorageDto);
        when(httpClientMongoOperator.find(query, ConnectorConstant.EXTERNAL_STORAGE_COLLECTION, ExternalStorageDto.class)).thenReturn(externalStorages);
        assertEquals(defaultExternalStorageDto, ExternalStorageUtil.getTapdataOrDefaultExternalStorage());
    }

}
