package io.tapdata.websocket.handler;


import com.tapdata.entity.Connections;
import io.tapdata.common.SettingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestConnectionHandlerTest {
    TestConnectionHandler handler;
    @BeforeEach
    void init() {
        handler = mock(TestConnectionHandler.class);
    }
    @Nested
    class SaveMongodbLoadSchemaSampleSizeTest {
        Map event;
        Connections connection;
        SettingService service;
        Object definitionTags;
        Map<String, Object> config;
        @BeforeEach
        void init() {
            config = mock(Map.class);
            event = mock(Map.class);
            connection = mock(Connections.class);
            service = mock(SettingService.class);

            when(event.get("connections.mongodbLoadSchemaSampleSize")).thenReturn(200);
            doNothing().when(connection).setSampleSize(anyInt());
            when(event.containsKey("definitionTags")).thenReturn(true);
            when(connection.getConfig()).thenReturn(config);
            when(event.get("definitionTags")).thenReturn(definitionTags);
            doNothing().when(connection).setTags(anyString());
        }
        void assertVerify(
                Map e, Connections c, SettingService s,
                int getIntTimes, int setSampleSizeTimes, int containsKeyTimes, int eventGetTimes, int setTagsTimes){
            handler.saveMongodbLoadSchemaSampleSize(e, c, s);
            verify(service, times(getIntTimes)).getInt("connections.mongodbLoadSchemaSampleSize", 100);
            verify(connection, times(setSampleSizeTimes)).setSampleSize(anyInt());
            verify(event, times(containsKeyTimes)).containsKey("definitionTags");
            verify(event, times(eventGetTimes)).get("definitionTags");
            verify(connection, times(setTagsTimes)).setTags(anyString());
        }

        @Test
        void testNormal() {
            doCallRealMethod().when(handler).saveMongodbLoadSchemaSampleSize(event, connection, service);
            List<String> definitionTagsArr = new ArrayList<>();
            definitionTagsArr.add("schema-free");
            when(event.get("definitionTags")).thenReturn(definitionTagsArr);
            assertVerify(event, connection, service,
                    1, 1, 1, 1, 1);
        }

        @Test
        void testNullConnections() {
            doCallRealMethod().when(handler).saveMongodbLoadSchemaSampleSize(event, null, service);
            definitionTags = new ArrayList<String>();
            ((List<String>)definitionTags).add("schema-free");
            assertVerify(event, null, service,
                    0, 0, 0, 0, 0);
        }

        @Test
        void testNullSettingService() {
            doCallRealMethod().when(handler).saveMongodbLoadSchemaSampleSize(event, connection, null);
            definitionTags = new ArrayList<String>();
            ((List<String>)definitionTags).add("schema-free");
            assertVerify(event, connection, null,
                    0, 0, 0, 0, 0);
        }

        @Test
        void testNullEvent() {
            doCallRealMethod().when(handler).saveMongodbLoadSchemaSampleSize(null, connection, service);
            definitionTags = new ArrayList<String>();
            ((List<String>)definitionTags).add("schema-free");
            assertVerify(null, connection, service,
                    1, 1, 0, 0, 0);
        }

        @Test
        void testEventNotDefinitionTags() {
            doCallRealMethod().when(handler).saveMongodbLoadSchemaSampleSize(event, connection, service);
            definitionTags = new ArrayList<String>();
            ((List<String>)definitionTags).add("schema-free");
            when(event.containsKey("definitionTags")).thenReturn(false);
            assertVerify(event, connection, service,
                    1, 1, 1, 0, 0);
        }

        @Test
        void testDefinitionTagsNotCollection() {
            doCallRealMethod().when(handler).saveMongodbLoadSchemaSampleSize(event, connection, service);
            definitionTags = 1;
            assertVerify(event, connection, service,
                    1, 1, 1, 1, 0);
        }
    }
}