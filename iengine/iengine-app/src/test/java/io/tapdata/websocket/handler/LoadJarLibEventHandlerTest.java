package io.tapdata.websocket.handler;

import com.mongodb.client.gridfs.GridFSBucket;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.websocket.WebSocketEventResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadJarLibEventHandlerTest {

    private ClientMongoOperator mockClientMongoOperator;

    private LoadJarLibEventHandler loadJarLibEventHandlerUnderTest;

    @BeforeEach
    public void setUp() {
        mockClientMongoOperator = mock(ClientMongoOperator.class);
        loadJarLibEventHandlerUnderTest = new LoadJarLibEventHandler();
        loadJarLibEventHandlerUnderTest.initialize(mockClientMongoOperator);
    }

    @Test
    public void testHandle() {
        final Map<String,String> event = new HashMap<>();
        event.put("fileId","656d7741e7cf041007e6fb8a");
        event.put("packageName","packageName");
        GridFSBucket gridFSBucket = mock(GridFSBucket.class);
        when(mockClientMongoOperator.getGridFSBucket()).thenReturn(gridFSBucket);
        WebSocketEventResult webSocketEventResult = loadJarLibEventHandlerUnderTest.handle(event);
        assertNotNull(webSocketEventResult);
    }
}
