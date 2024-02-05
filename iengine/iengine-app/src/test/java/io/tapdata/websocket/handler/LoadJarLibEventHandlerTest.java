package io.tapdata.websocket.handler;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.websocket.WebSocketEventResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LoadJarLibEventHandlerTest {

    @Mock
    private ClientMongoOperator mockClientMongoOperator;

    private LoadJarLibEventHandler loadJarLibEventHandlerUnderTest;

    @Before
    public void setUp() {
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
        Assert.assertNotNull(webSocketEventResult);
    }
}
