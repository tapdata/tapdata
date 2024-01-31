package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.sharecdc.LogContent;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-31 12:07
 **/
@DisplayName("HazelcastTargetPdkShareCDCNode Class Test")
class HazelcastTargetPdkShareCDCNodeTest {

	private HazelcastTargetPdkShareCDCNode hazelcastTargetPdkShareCDCNode;

	@BeforeEach
	void setUp() {
		hazelcastTargetPdkShareCDCNode = mock(HazelcastTargetPdkShareCDCNode.class);
	}

	@Nested
	@DisplayName("writeLogContent method test")
	class writeLogContentTest {

		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkShareCDCNode).writeLogContent(any(LogContent.class));
		}

		@Test
		@DisplayName("main process test")
		@SneakyThrows
		void testMainProcess() {
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			LogContent logContent = new LogContent("test", data, null, null, System.currentTimeMillis());
			logContent.setConnectionId("1");
			try (
					MockedStatic<ShareCdcUtil> shareCdcUtilMockedStatic = mockStatic(ShareCdcUtil.class)
			) {
				shareCdcUtilMockedStatic.when(() -> ShareCdcUtil.getTableId(logContent)).thenReturn("test_table_id");
				HazelcastConstruct hazelcastConstruct = mock(HazelcastConstruct.class);
				when(hazelcastConstruct.insert(any(Document.class))).thenReturn(1);
				when(hazelcastTargetPdkShareCDCNode.getConstruct(anyString(), anyString(), anyString()))
						.thenAnswer(invocationOnMock -> {
							Object argument1 = invocationOnMock.getArgument(0);
							assertEquals("test_table_id", argument1);
							return hazelcastConstruct;
						});
				hazelcastTargetPdkShareCDCNode.writeLogContent(logContent);
			}
		}

		@Test
		@DisplayName("when construct insert error")
		@SneakyThrows
		void whenConstructInsertError() {
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			LogContent logContent = new LogContent("test", data, null, null, System.currentTimeMillis());
			logContent.setConnectionId("1");
			try (
					MockedStatic<ShareCdcUtil> shareCdcUtilMockedStatic = mockStatic(ShareCdcUtil.class)
			) {
				shareCdcUtilMockedStatic.when(() -> ShareCdcUtil.getTableId(logContent)).thenReturn("test_table_id");
				HazelcastConstruct hazelcastConstruct = mock(HazelcastConstruct.class);
				RuntimeException runtimeException = new RuntimeException("test error");
				when(hazelcastConstruct.insert(any(Document.class))).thenThrow(runtimeException);
				when(hazelcastTargetPdkShareCDCNode.getConstruct(anyString(), anyString(), anyString()))
						.thenAnswer(invocationOnMock -> {
							Object argument1 = invocationOnMock.getArgument(0);
							assertEquals("test_table_id", argument1);
							return hazelcastConstruct;
						});
				TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastTargetPdkShareCDCNode.writeLogContent(logContent));
				assertEquals(TaskProcessorExCode_11.WRITE_ONE_SHARE_LOG_FAILED, tapCodeException.getCode());
				assertEquals(runtimeException, tapCodeException.getCause());
			}
		}
	}
}
