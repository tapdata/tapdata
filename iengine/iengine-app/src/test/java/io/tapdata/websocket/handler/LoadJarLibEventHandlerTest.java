package io.tapdata.websocket.handler;

import com.mongodb.client.gridfs.GridFSBucket;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.websocket.WebSocketEventResult;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LoadJarLibEventHandlerTest {

	@Mock
	private ClientMongoOperator mockClientMongoOperator;

	private LoadJarLibEventHandler loadJarLibEventHandlerUnderTest;

	@BeforeEach
	public void setUp() {
		mockClientMongoOperator = mock(ClientMongoOperator.class);
		loadJarLibEventHandlerUnderTest = new LoadJarLibEventHandler();
		loadJarLibEventHandlerUnderTest.initialize(mockClientMongoOperator);
	}

	@Test
	void testHandle() {
		final Map<String, String> event = new HashMap<>();
		event.put("fileId", "656d7741e7cf041007e6fb8a");
		event.put("packageName", "packageName");
		GridFSBucket gridFSBucket = mock(GridFSBucket.class);
		when(mockClientMongoOperator.getGridFSBucket()).thenReturn(gridFSBucket);
		WebSocketEventResult webSocketEventResult = loadJarLibEventHandlerUnderTest.handle(event);
		Assert.assertNotNull(webSocketEventResult);
	}
	@Test
	void test() {
		String fileId = "656d7741e7cf041007e6fb8a";
		Path path = Paths.get("/null");
//		URL url = path.toUri().toURL();
        try {
            System.out.println(path.toUri().toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
