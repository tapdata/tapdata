package com.tapdata.tm.schedule.util;


import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.ScheduleTimeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoadSchemaScheduleUtilTest {
    LoadSchemaScheduleUtil scheduleUtil;
    @BeforeEach
    void init() {
        scheduleUtil = mock(LoadSchemaScheduleUtil.class);
    }

    @Nested
    class ParamsTest {
        @Test
        void testParams() {
            Assertions.assertEquals("connection_schema_update_hour", LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR);
            Assertions.assertEquals("2:00", LoadSchemaScheduleUtil.DEFAULT_UPDATE_HOUR);
            Assertions.assertEquals("connection_schema_update_interval", LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL);
            Assertions.assertEquals(1, LoadSchemaScheduleUtil.DEFAULT_UPDATE_INTERVAL);
            Assertions.assertNotNull(LoadSchemaScheduleUtil.keys);
            Assertions.assertEquals(2, LoadSchemaScheduleUtil.keys.length);
            Assertions.assertEquals("connection_schema_update_hour", LoadSchemaScheduleUtil.keys[0]);
            Assertions.assertEquals("connection_schema_update_interval", LoadSchemaScheduleUtil.keys[1]);
        }
    }

    @Nested
    class CriteriaTest {
        @Test
        void testNormal() {
            when(scheduleUtil.criteria(anyString())).thenCallRealMethod();
            Criteria mockId = scheduleUtil.criteria("mockId");
            Assertions.assertNotNull(mockId);
            Document criteriaObject = mockId.getCriteriaObject();
            Assertions.assertNotNull(criteriaObject);
            Assertions.assertEquals(5, criteriaObject.size());
            Assertions.assertTrue(criteriaObject.containsKey("is_deleted"));
            Assertions.assertTrue(criteriaObject.containsKey("source._id"));
            Assertions.assertTrue(criteriaObject.containsKey("sourceType"));
            Assertions.assertTrue(criteriaObject.containsKey("meta_type"));
            Assertions.assertTrue(criteriaObject.containsKey("taskId"));
        }
    }

    @Nested
    class WhereForSettingsTest {
        String[] keys;
        @BeforeEach
        void init() {
            keys = new String[]{"key"};
            when(scheduleUtil.whereForSettings(any(String[].class))).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            Query query = scheduleUtil.whereForSettings(keys);
            Assertions.assertNotNull(query);
        }
        @Test
        void testNullKeys() {
            when(scheduleUtil.whereForSettings(null)).thenCallRealMethod();
            Query query = scheduleUtil.whereForSettings(null);
            Assertions.assertNotNull(query);
        }
        @Test
        void testEmptyKeys() {
            keys = new String[0];
            when(scheduleUtil.whereForSettings(keys)).thenCallRealMethod();
            Query query = scheduleUtil.whereForSettings(keys);
            Assertions.assertNotNull(query);
        }
    }

    @Nested
    class GetStringFromSettingTest {
        SettingsDto settings;
        String defaultValue;
        @BeforeEach
        void init() {
            settings = mock(SettingsDto.class);
            defaultValue = "default";

            when(scheduleUtil.getStringFromSetting(any(SettingsDto.class), anyString())).thenCallRealMethod();
            when(scheduleUtil.getStringFromSetting(null, defaultValue)).thenCallRealMethod();
        }
        String assertVerify(SettingsDto settingsTemp, int getValueTimes) {
            try {
                return scheduleUtil.getStringFromSetting(settingsTemp, defaultValue);
            } finally {
                verify(settings, times(getValueTimes)).getValue();
            }
        }
        @Test
        void testNormal() {
            when(settings.getValue()).thenReturn("12");
            String result = assertVerify(settings, 1);
            Assertions.assertEquals("12", result);
        }
        @Test
        void testNullSetting() {
            when(settings.getValue()).thenReturn("12");
            String result = assertVerify(null, 0);
            Assertions.assertEquals(defaultValue, result);
        }
        @Test
        void testOtherValue() {
            when(settings.getValue()).thenReturn(new HashMap<>());
            String result = assertVerify(settings, 1);
            Assertions.assertEquals(defaultValue, result);
        }
        @Test
        void testNullValue() {
            when(settings.getValue()).thenReturn(null);
            String result = assertVerify(settings, 1);
            Assertions.assertEquals(defaultValue, result);
        }

    }

    @Nested
    class GetIntValueFromSettingTest {
        SettingsDto settings;
        int defaultValue;
        @BeforeEach
        void init() {
            settings = mock(SettingsDto.class);
            defaultValue = 1;

            when(scheduleUtil.getIntValueFromSetting(any(SettingsDto.class), anyInt())).thenCallRealMethod();
            when(scheduleUtil.getIntValueFromSetting(null, defaultValue)).thenCallRealMethod();
        }
        int assertVerify(SettingsDto settingsTemp, int getValueTimes) {
            try {
                return scheduleUtil.getIntValueFromSetting(settingsTemp, defaultValue);
            } finally {
                verify(settings, times(getValueTimes)).getValue();
            }
        }
        @Test
        void testNormal() {
            when(settings.getValue()).thenReturn(12);
            int result = assertVerify(settings, 1);
            Assertions.assertEquals(12, result);
        }
        @Test
        void testNullSetting() {
            when(settings.getValue()).thenReturn(12);
            int result = assertVerify(null, 0);
            Assertions.assertEquals(defaultValue, result);
        }
        @Test
        void testStringValue() {
            when(settings.getValue()).thenReturn("12");
            int result = assertVerify(settings, 1);
            Assertions.assertEquals(12, result);
        }
        @Test
        void testStringButNotNumberStringValue() {
            when(settings.getValue()).thenReturn("12hh");
            int result = assertVerify(settings, 1);
            Assertions.assertEquals(defaultValue, result);
        }
        @Test
        void testOtherValue() {
            when(settings.getValue()).thenReturn(new HashMap<>());
            int result = assertVerify(settings, 1);
            Assertions.assertEquals(defaultValue, result);
        }
        @Test
        void testNullValue() {
            when(settings.getValue()).thenReturn(null);
            int result = assertVerify(settings, 1);
            Assertions.assertEquals(defaultValue, result);
        }
    }

    @Nested
    class NowTest {
        @Test
        void testNormal() {
            when(scheduleUtil.now()).thenCallRealMethod();
            long now = scheduleUtil.now();
            Assertions.assertTrue(now <= System.currentTimeMillis());
        }
    }
    @Nested
    class SleepTest {
        @BeforeEach
        void init() throws InterruptedException {
            doCallRealMethod().when(scheduleUtil).sleep(anyLong());
            doNothing().when(scheduleUtil).sleepMill(anyLong());
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(()-> {
                scheduleUtil.sleep(33);
                verify(scheduleUtil, times(1)).sleepMill(33000);
            });
        }
        @Test
        void testMoreThan60() {
            Assertions.assertDoesNotThrow(()-> {
                scheduleUtil.sleep(83);
                verify(scheduleUtil, times(1)).sleepMill(60000);
            });
        }
        @Test
        void testException() throws InterruptedException {
            doAnswer(e -> {throw new InterruptedException("Failed");}).when(scheduleUtil).sleepMill(anyLong());
            Assertions.assertDoesNotThrow(()-> {
                scheduleUtil.sleep(83);
                verify(scheduleUtil, times(1)).sleepMill(60000);
            });
        }
    }
    @Nested
    class SleepMillTest {
        @BeforeEach
        void init() throws InterruptedException {
            doCallRealMethod().when(scheduleUtil).sleepMill(anyLong());
        }
        @Test
        void testException() throws InterruptedException {
            Assertions.assertDoesNotThrow(()-> {
                scheduleUtil.sleepMill(1);
            });
        }
    }
    @Nested
    class GetUpdateHourTest {
        @BeforeEach
        void init() {
            when(scheduleUtil.getUpdateHour(anyString(), anyInt())).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            int updateHour = scheduleUtil.getUpdateHour("5:00", 2);
            Assertions.assertEquals(5, updateHour);
        }
        @Test
        void testNullUpdateHourStr() {
            when(scheduleUtil.getUpdateHour(null, 2)).thenCallRealMethod();
            int updateHour = scheduleUtil.getUpdateHour(null, 2);
            Assertions.assertEquals(-10, updateHour);
        }
        @Test
        void testNotContainsCharInUpdateHourStr() {
            when(scheduleUtil.getUpdateHour("03", 2)).thenCallRealMethod();
            int updateHour = scheduleUtil.getUpdateHour("03", 2);
            Assertions.assertEquals(3, updateHour);
        }
        @Test
        void testNotNumberStringInUpdateHourStr() {
            when(scheduleUtil.getUpdateHour("0x2", 2)).thenCallRealMethod();
            int updateHour = scheduleUtil.getUpdateHour("0x2", 2);
            Assertions.assertEquals(-10, updateHour);
        }
        @Test
        void testContainsCharButNotNumberStringInUpdateHourStr() {
            when(scheduleUtil.getUpdateHour("0x2:00", 2)).thenCallRealMethod();
            int updateHour = scheduleUtil.getUpdateHour("0x2:00", 2);
            Assertions.assertEquals(-10, updateHour);
        }
        @Test
        void testContainsCharIsFalseStringInUpdateHourStr() {
            when(scheduleUtil.getUpdateHour("false", 2)).thenCallRealMethod();
            int updateHour = scheduleUtil.getUpdateHour("false", 2);
            Assertions.assertEquals(100, updateHour);
        }
    }

    @Nested
    class LoadSchemaOnceTest {
        LocalDateTime now;
        Date loadSchemaTime;
        DataSourceConnectionDto dataSource;
        DataSourceService service;
        UserDetail user;
        long count;
        int interval;
        int time;
        long nowTime;
        @BeforeEach
        void init() {
            nowTime = System.currentTimeMillis();
            now = mock(LocalDateTime.class);
            dataSource = mock(DataSourceConnectionDto.class);
            service = mock(DataSourceService.class);
            user = mock(UserDetail.class);
            loadSchemaTime = mock(Date.class);

            count = 1000L;
            time = 2;
            interval = 1;

            when(now.getHour()).thenReturn(time);
            when(scheduleUtil.now()).thenReturn(nowTime);
            when(dataSource.getLoadSchemaTime()).thenReturn(loadSchemaTime);
            when(loadSchemaTime.getTime()).thenReturn(0L);

            doNothing().when(service).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), any(UserDetail.class));
            doNothing().when(scheduleUtil).sleep(anyLong());

            doCallRealMethod().when(scheduleUtil).loadSchemaOnce(any(DataSourceConnectionDto.class), anyLong(),anyInt(), anyInt(), any(UserDetail.class), any(DataSourceService.class));
        }

        void assertVerify(
                DataSourceConnectionDto dataSourceTemp, DataSourceService serviceTemp,
                int nowTimes, int getHourTimes, int nowFuncTimes,
                int getLoadSchemaTimeTimes, int getTimeTimes,
                int sendTestConnectionTimes, int sleepTimes) {
            try (MockedStatic<LocalDateTime> mockedStatic = mockStatic(LocalDateTime.class)){
                mockedStatic.when(LocalDateTime::now).thenReturn(now);
                scheduleUtil.loadSchemaOnce(dataSourceTemp, count, interval, time, user, serviceTemp);
                mockedStatic.verify(LocalDateTime::now, times(nowTimes));
            } finally {
                verify(now, times(getHourTimes)).getHour();
                verify(dataSource, times(getLoadSchemaTimeTimes)).getLoadSchemaTime();
                verify(loadSchemaTime, times(getTimeTimes)).getTime();
                verify(scheduleUtil, times(nowFuncTimes)).now();
                verify(service, times(sendTestConnectionTimes)).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), any(UserDetail.class));
                verify(scheduleUtil, times(sleepTimes)).sleep(anyLong());
            }
        }


        @Test
        void testNullDataSourceConnectionDto() {
            doCallRealMethod().when(scheduleUtil).loadSchemaOnce(null, count, interval, time, user, service);
            assertVerify(null, service,
                    0, 0, 0,
                    0, 0,
                    0, 0);
        }
        @Test
        void testNullDataSourceService() {
            doCallRealMethod().when(scheduleUtil).loadSchemaOnce(dataSource, count, interval, time, user, null);
            assertVerify(dataSource, null,
                    0, 0, 0,
                    0, 0,
                    0, 0);
        }
        @Test
        void testNullWhenGetLoadSchemaTime() {
            when(dataSource.getLoadSchemaTime()).thenReturn(null);
            assertVerify(dataSource, service,
                    1, 1, 1,
                    1, 0,
                    1, 1);
        }
        @Test
        void testTimeNotEqualHour() {
            when(now.getHour()).thenReturn(3);
            assertVerify(dataSource, service,
                    1, 1, 0,
                    1, 1,
                    0, 0);
        }

        @Test
        void testMoreThanInterval() {
            assertVerify(dataSource, service,
                    1, 1, 1,
                    1, 1,
                    1, 1);
        }

        @Test
        void testLessThanInterval() {
            when(scheduleUtil.now()).thenReturn(0L);
            assertVerify(dataSource, service,
                    1, 1, 1,
                    1, 1,
                    0, 0);
        }

        @Test
        void testEqualsInterval() {
            when(loadSchemaTime.getTime()).thenReturn(nowTime - interval * 24 * 60 * 60 * 1000);
            assertVerify(dataSource, service,
                    1, 1, 1,
                    1, 1,
                    1, 1);
        }

        @Test
        void testTimeNotEqualHourAndLastLoadTimeLessThanInterval() {
            when(now.getHour()).thenReturn(3);
            when(loadSchemaTime.getTime()).thenReturn(nowTime - interval * 24 * 60 * 60 * 1000 - 1);
            assertVerify(dataSource, service,
                    1, 1, 0,
                    1, 1,
                    0, 0);
        }

    }


    @Nested
    class DoLoadSchemaTest {
        DataSourceConnectionDto dataSource;
        Map<String, UserDetail> userDetailMap;
        MetadataInstancesService metadataInstancesService;
        Map<String, Object> settingMap;
        DataSourceService dataSourceService;

        UserDetail user;
        Criteria criteria;

        ObjectId id;
        String idStr;
        String userId;
        long count;
        @BeforeEach
        void init() {
            userId = "userId";
            idStr = "idStr";

            criteria = mock(Criteria.class);
            user = mock(UserDetail.class);
            id = mock(ObjectId.class);
            dataSource = mock(DataSourceConnectionDto.class);
            userDetailMap = mock(Map.class);
            metadataInstancesService = mock(MetadataInstancesService.class);
            settingMap = mock(Map.class);
            dataSourceService = mock(DataSourceService.class);


            when(dataSource.getSchemaUpdateHour()).thenReturn("02:00");
            when(dataSource.getUserId()).thenReturn(userId);
            when(dataSource.getId()).thenReturn(id);

            when(id.toHexString()).thenReturn(idStr);
            when(userDetailMap.get(userId)).thenReturn(user);
            when(scheduleUtil.criteria(anyString())).thenReturn(criteria);
            when(metadataInstancesService.count(any(Query.class))).thenReturn(count);

            when(settingMap.get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL)).thenReturn(1);
            when(settingMap.get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR)).thenReturn(2);

            doNothing().when(scheduleUtil).loadSchemaOnce(
                    any(DataSourceConnectionDto.class),
                    anyLong(),
                    anyInt(),
                    anyInt(),
                    any(UserDetail.class),
                    any(DataSourceService.class)
            );

            doCallRealMethod().when(scheduleUtil).doLoadSchema(dataSource, userDetailMap, metadataInstancesService, settingMap, dataSourceService);
        }

        void assertVerify(int getUserIdTimes, int criteriaTimes, int countTimes,
                          int getSchemaUpdateHourTimes,
                          int getIntervalTimes, int getHourTimes, int loadSchemaOnceTimes) {
            scheduleUtil.doLoadSchema(dataSource, userDetailMap, metadataInstancesService, settingMap, dataSourceService);

            verify(dataSource, times(getUserIdTimes)).getUserId();
            verify(userDetailMap, times(getUserIdTimes)).get(anyString());

            verify(id, times(criteriaTimes)).toHexString();
            verify(dataSource, times(criteriaTimes)).getId();
            verify(scheduleUtil, times(criteriaTimes)).criteria(anyString());

            verify(metadataInstancesService, times(countTimes)).count(any(Query.class));

            verify(dataSource, times(getSchemaUpdateHourTimes)).getSchemaUpdateHour();
            verify(settingMap, times(getIntervalTimes)).get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL);
            verify(settingMap, times(getHourTimes)).get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR);

            verify(scheduleUtil, times(loadSchemaOnceTimes)).loadSchemaOnce(
                    any(DataSourceConnectionDto.class),
                    anyLong(),
                    anyInt(),
                    anyInt(),
                    any(UserDetail.class),
                    any(DataSourceService.class)
            );
        }

        @Test
        void testNormal() {
            assertVerify(1, 1, 1, 1, 1, 0, 1);
        }

        @Test
        void testNullInterval() {
            when(settingMap.get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL)).thenReturn(null);
            assertVerify(1, 1, 1, 1, 1, 0, 1);
        }

        @Test
        void testGetSchemaUpdateHourFalse() {
            when(dataSource.getSchemaUpdateHour()).thenReturn("false");
            assertVerify(1, 1, 1, 1, 1, 0, 0);
        }

        @Test
        void testGetSchemaUpdateHourDefault() {
            when(dataSource.getSchemaUpdateHour()).thenReturn("default");
            assertVerify(1, 1, 1, 1, 1, 1, 1);
        }

        @Test
        void testGetSchemaUpdateHourDefaultAndNullHour() {
            when(dataSource.getSchemaUpdateHour()).thenReturn("default");
            when(settingMap.get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR)).thenReturn(null);
            assertVerify(1, 1, 1, 1, 1, 1, 1);
        }

        @Test
        void testGetSchemaUpdateHourDefaultAndHourAndDefaultIsClose() {
            when(dataSource.getSchemaUpdateHour()).thenReturn("default");
            when(settingMap.get(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR)).thenReturn(ScheduleTimeEnum.FALSE.getValue());
            assertVerify(1, 1, 1, 1, 1, 1, 0);
        }
    }

    @Nested
    class LoadSettingsTest {
        SettingsService settingsService;
        Map<String, Object> settingMap;
        List<SettingsDto> settings;
        Query query;
        SettingsDto settingsDto;
        String key;

        @BeforeEach
        void init() {
            settingsService = mock(SettingsService.class);
            settingMap = mock(Map.class);
            settings = new ArrayList<>();
            query = mock(Query.class);
            settingsDto = mock(SettingsDto.class);
            settings.add(settingsDto);
            key = "key";

            when(scheduleUtil.whereForSettings(LoadSchemaScheduleUtil.keys)).thenReturn(query);
            when(settingsService.findALl("1", query)).thenReturn(settings);

            when(settingsDto.getKey()).thenReturn(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR);

            when(scheduleUtil.getStringFromSetting(settingsDto, LoadSchemaScheduleUtil.DEFAULT_UPDATE_HOUR)).thenReturn("2:00");
            when(scheduleUtil.getUpdateHour("2:00", ScheduleTimeEnum.TWO.getValue())).thenReturn(2);
            when(settingMap.put(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR, 2)).thenReturn(null);

            when(scheduleUtil.getIntValueFromSetting(settingsDto, LoadSchemaScheduleUtil.DEFAULT_UPDATE_INTERVAL)).thenReturn(1);
            when(settingMap.put(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL, 1)).thenReturn(null);

            doCallRealMethod().when(scheduleUtil).loadSettings(settingsService, settingMap);
        }

        void assertVerify(SettingsService service, Map<String, Object> map,
                          int findALlTimes, int getKeyTimes,
                          int getStringFromSettingTimes,
                          int getUpdateHourTimes, int getIntValueFromSettingTimes,
                          int putHourTimes, int putIntervalTimes) {
            scheduleUtil.loadSettings(service, map);
            verify(scheduleUtil, times(findALlTimes)).whereForSettings(LoadSchemaScheduleUtil.keys);
            verify(settingsService, times(findALlTimes)).findALl("1", query);
            verify(settingsDto, times(getKeyTimes)).getKey();
            verify(scheduleUtil, times(getStringFromSettingTimes)).getStringFromSetting(settingsDto, LoadSchemaScheduleUtil.DEFAULT_UPDATE_HOUR);
            verify(scheduleUtil, times(getUpdateHourTimes)).getUpdateHour("2:00", ScheduleTimeEnum.TWO.getValue());
            verify(settingMap, times(putHourTimes)).put(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_HOUR, 2);

            verify(scheduleUtil, times(getIntValueFromSettingTimes)).getIntValueFromSetting(settingsDto, LoadSchemaScheduleUtil.DEFAULT_UPDATE_INTERVAL);
            verify(settingMap, times(putIntervalTimes)).put(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL, 1);
        }

        @Test
        void testNormal() {
            assertVerify(settingsService, settingMap, 1, 1, 1, 1, 0 , 1, 0);
        }

        @Test
        void testInterval() {
            when(settingsDto.getKey()).thenReturn(LoadSchemaScheduleUtil.CONNECTION_SCHEMA_UPDATE_INTERVAL);
            assertVerify(settingsService, settingMap, 1, 1, 0, 0, 1 , 0, 1);
        }
        @Test
        void testOtherKey() {
            when(settingsDto.getKey()).thenReturn("otherKey");
            assertVerify(settingsService, settingMap, 1, 1, 0, 0, 0 , 0, 0);
        }
        @Test
        void testNullSettingsService() {
            doCallRealMethod().when(scheduleUtil).loadSettings(null, settingMap);
            assertVerify(null, settingMap, 0, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void testNullSettingMap() {
            doCallRealMethod().when(scheduleUtil).loadSettings(settingsService, null);
            assertVerify(settingsService, null, 0, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void testNullSettings() {
            when(settingsService.findALl("1", query)).thenReturn(null);
            assertVerify(settingsService, settingMap, 1, 0, 0, 0, 0, 0, 0);
        }
    }
}