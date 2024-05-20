package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-05-18 17:22
 **/
@DisplayName("Class HazelcastProcessorBaseNode Test")
class HazelcastProcessorBaseNodeTest {

	private HazelcastProcessorBaseNode hazelcastProcessorBaseNode;

	@BeforeEach
	void setUp() {
		hazelcastProcessorBaseNode = mock(HazelcastProcessorBaseNode.class);
	}

	@Nested
	@DisplayName("Method initFilterCodec test")
	class initFilterCodecTest {
		@BeforeEach
		void setUp() {
			when(hazelcastProcessorBaseNode.initFilterCodec()).thenCallRealMethod();
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			TapCodecsFilterManager tapCodecsFilterManager = hazelcastProcessorBaseNode.initFilterCodec();
			assertNotNull(tapCodecsFilterManager);
			TapCodecsRegistry codecsRegistry = tapCodecsFilterManager.getCodecsRegistry();
			assertNotNull(codecsRegistry);
			Object classFromTapValueCodecMap = ReflectionTestUtils.getField(codecsRegistry, "classFromTapValueCodecMap");
			assertInstanceOf(ConcurrentHashMap.class, classFromTapValueCodecMap);
			assertTrue(((ConcurrentHashMap<?, ?>) classFromTapValueCodecMap).isEmpty());
			ToTapValueCodec<?> customToTapValueCodec = codecsRegistry.getCustomToTapValueCodec(byte[].class);
			assertNotNull(customToTapValueCodec);
		}
	}

}