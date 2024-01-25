package io.tapdata.sharecdc;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.constructImpl.ConstructRingBuffer;
import io.tapdata.flow.engine.V2.common.StoreLoggerImpl;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcPDKTaskReader;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.data.mongodb.core.query.Criteria.where;

public class ShareCdcPDKTaskReaderTest {

    static TaskDto taskDto;
    static String tableName;

    static ObsLogger obsLogger;

    static ShareCdcPDKTaskReader shareCdcPDKTaskReader;

    static ShareCdcContext shareCdcContext;

    @BeforeAll
    public static void init() {
        shareCdcPDKTaskReader = Mockito.mock(ShareCdcPDKTaskReader.class);
        taskDto = new TaskDto();
        taskDto.setName("test");
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "logCollectorTaskDto", taskDto);

        shareCdcContext = Mockito.mock(ShareCdcTaskPdkContext.class);
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "shareCdcContext", shareCdcContext);
        when(((ShareCdcTaskPdkContext) shareCdcContext).getConnections()).thenReturn(new Connections());
        when(((ShareCdcTaskPdkContext) shareCdcContext).getTaskDto()).thenReturn(taskDto);

        ClientMongoOperator clientMongoOperator = Mockito.mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "clientMongoOperator", clientMongoOperator);

        tableName = "testConstruct";
        obsLogger = Mockito.mock(ObsLogger.class);
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "obsLogger", obsLogger);

        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "hazelcastInstance", hazelcastInstance);

        Query query = Query.query(where("sign").is("null_" + tableName));
        when(clientMongoOperator.findOne(query, ConnectorConstant.SHARE_CDC_TABLE_MAPPING_COLLECTION,
                ShareCdcTableMappingDto.class)).thenReturn(new ShareCdcTableMappingDto());

    }

    @Test
    public void getConstructForRocksdbTest() {
        ExternalStorageDto logCollectorExternalStorage = new ExternalStorageDto();
        logCollectorExternalStorage.setType("rocksdb");
        logCollectorExternalStorage.setUri("/data/test");
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "logCollectorExternalStorage", logCollectorExternalStorage);
        try (MockedStatic<ExternalStorageUtil> data = Mockito
                .mockStatic(ExternalStorageUtil.class)) {
            data.when(() -> ExternalStorageUtil.initHZRingBufferStorage(logCollectorExternalStorage,
                    null, "null_" + tableName, null, new StoreLoggerImpl(obsLogger))).thenAnswer((Answer<Void>) invocation -> null);
            ;
            ConstructRingBuffer constructRingBuffer = ReflectionTestUtils.invokeMethod(shareCdcPDKTaskReader, "getConstruct", tableName);
            String actualData = (String) ReflectionTestUtils.getField(constructRingBuffer, "name");
            String exceptData = ShareCdcUtil.getConstructName(taskDto, tableName);
            assertEquals(exceptData, actualData);
        }
    }

    @Test
    public void getConstructForMongoDbTest() {

        ExternalStorageDto logCollectorExternalStorage = new ExternalStorageDto();
        logCollectorExternalStorage.setType("mongodb");
        logCollectorExternalStorage.setUri("/data/test");
        ReflectionTestUtils.setField(shareCdcPDKTaskReader, "logCollectorExternalStorage", logCollectorExternalStorage);
        try (MockedStatic<ExternalStorageUtil> data = Mockito
                .mockStatic(ExternalStorageUtil.class)) {
            data.when(() -> ExternalStorageUtil.initHZRingBufferStorage(logCollectorExternalStorage,
                    null, "null_" + tableName, null, new StoreLoggerImpl(obsLogger))).thenAnswer((Answer<Void>) invocation -> null);
            ConstructRingBuffer constructRingBuffer = ReflectionTestUtils.invokeMethod(shareCdcPDKTaskReader, "getConstruct", tableName);
            String actualData = (String) ReflectionTestUtils.getField(constructRingBuffer, "name");
            String exceptData = ShareCdcUtil.getConstructName(taskDto, tableName) + "_" + ((ShareCdcTaskPdkContext) shareCdcContext).getTaskDto().getName();
            assertEquals(exceptData, actualData);
        }
    }
}
