package io.tapdata.Runnable;

import com.tapdata.entity.Connections;

import java.util.Map;

import com.tapdata.entity.DatabaseTypeEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadSchemaRunnerTest {
    LoadSchemaRunner runner;
    @BeforeEach
    void init() {
        runner = mock(LoadSchemaRunner.class);
    }

    @Nested
    class ConfigMongodbLoadSchemaSampleSizeTest {
        Connections connections;
        Map<String, Object> config;
        DatabaseTypeEnum.DatabaseType databaseType;
        @BeforeEach
        void init() {
            databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
            connections = mock(Connections.class);
            config = mock(Map.class);

            when(databaseType.getPdkId()).thenReturn("mongodb");
            when(connections.getConfig()).thenReturn(config);
            when(connections.getTags()).thenReturn("schema-free");
            doNothing().when(connections).setConfig(any(Map.class));
            when(connections.getSampleSize()).thenReturn(1000);
            when(config.put("mongodbLoadSchemaSampleSize", 1000)).thenReturn(null);

            doCallRealMethod().when(runner).configMongodbLoadSchemaSampleSize(connections, databaseType);
        }
        void assertVerify(Connections connectionsTemp,
                          DatabaseTypeEnum.DatabaseType databaseTypeTemp,
                          int sampleSize,
                          int getTagsTimes,
                          int getPdkIdTimes,
                          int getConfigTimes,
                          int setConfigTimes,
                          int getSampleSizeTimes,
                          int putTimes) {
            runner.configMongodbLoadSchemaSampleSize(connectionsTemp, databaseTypeTemp);
            verify(connections, times(getTagsTimes)).getTags();
            verify(connections, times(getConfigTimes)).getConfig();
            verify(databaseType, times(getPdkIdTimes)).getPdkId();
            verify(connections, times(setConfigTimes)).setConfig(anyMap());
            verify(connections, times(getSampleSizeTimes)).getSampleSize();
            verify(config, times(putTimes)).put("mongodbLoadSchemaSampleSize", sampleSize);
        }

        @Test
        void testNormal() {
            assertVerify(connections, databaseType, 1000, 1, 1, 1, 0, 1, 1);
        }
        @Test
        void testNullConfigMap() {
            when(connections.getConfig()).thenReturn(null);
            assertVerify(connections, databaseType, 1000, 1, 1,1, 1, 1, 0);
        }
        @Test
        void testSampleSizeLessThanZero() {
            when(connections.getSampleSize()).thenReturn(-1);
            assertVerify(connections, databaseType, 100, 1, 1,1, 0, 1, 1);
        }
        @Test
        void testNullDataType() {
            doCallRealMethod().when(runner).configMongodbLoadSchemaSampleSize(connections, null);
            assertVerify(connections, null, 100, 0,0, 0, 0, 0, 0);
        }
        @Test
        void testNotMongoDB() {
            when(databaseType.getPdkId()).thenReturn("mysql");
            assertVerify(connections, databaseType, 100, 1,1, 1, 0, 1, 0);
        }

        @Test
        void testIsSchemaFree() {
            when(databaseType.getPdkId()).thenReturn("mysql");
            when(connections.getTags()).thenReturn("schema-free");
            assertVerify(connections, databaseType, 1000, 1,1, 1, 0, 1, 1);
        }

        @Test
        void testNotSchemaFree() {
            when(databaseType.getPdkId()).thenReturn("mysql");
            when(connections.getTags()).thenReturn("sdhema-free");
            assertVerify(connections, databaseType, 1000, 1,1, 0, 0, 0, 0);
        }
        @Test
        void testTagIsNull() {
            when(databaseType.getPdkId()).thenReturn("mysql");
            when(connections.getTags()).thenReturn(null);
            assertVerify(connections, databaseType, 100, 1,1, 0, 0, 0, 0);
        }
    }
}