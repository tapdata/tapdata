package io.tapdata.websocket.handler;

import com.tapdata.entity.Connections;
import io.tapdata.flow.engine.V2.index.TwoDbRedlineViolationException;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.websocket.SendMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * 连接级 PDK 动作的公共生命周期（{@link ConnectionScopedPdkHandler}）——TAP-12057 · P3 收尾重构。
 *
 * <p>{@code QueryIndexesHandler}（读）与 {@code CreateIndexHandler}（写）各自复制过一份
 * 「红线 → 建节点 → INIT → 动作 → STOP → 释放 associateId」，而两边的单测都把这整段 spy 掉了，
 * 于是它<b>一行覆盖都没有</b>。抽公共基类前先在这里把它钉住：没有网就不该在钢丝上重构。</p>
 *
 * <p>钉的是<b>收尾语义</b>——动作失败也要 STOP、STOP 失败不能倒打成功为失败、红线在建节点之前、
 * associateId 申请了就必须还。这几条错一条都不会被现有测试发现，却会在生产上泄漏连接器句柄。</p>
 */
class ConnectionScopedPdkHandlerTest {

	private static final String PLATFORM_URI = "mongodb://platform:27017/tapdata";

	/** 只暴露基类能力的探针；事件解析是各 handler 自己的事，不在本测试范围。 */
	static class ProbeHandler extends ConnectionScopedPdkHandler {
		@Override
		public Object handle(Map event, SendMessage sendMessage) {
			return null;
		}

		@Override
		protected String pdkTag() {
			return "probe";
		}
	}

	private ProbeHandler handler;
	private ConnectorNode node;
	private List<String> trace;

	@BeforeEach
	void setUp() throws Throwable {
		handler = spy(new ProbeHandler());
		node = mock(ConnectorNode.class);
		trace = new ArrayList<>();
		doReturn(PLATFORM_URI).when(handler).platformMongoUri();
		// 建节点触达 Hazelcast / PDK 包下载，属集成范畴——本测试只管它之后的编排。
		doReturn(node).when(handler).buildNode(any(Connections.class), anyString());
	}

	private Connections userDb() {
		Connections conn = new Connections();
		conn.setId("c1");
		conn.setName("user-conn");
		conn.setDatabase_uri("mongodb://user:27017/biz");
		return conn;
	}

	/** 让 INIT/STOP 真的执行传进去的动作，并按序记账。 */
	private void recordLifecycle(MockedStatic<PDKInvocationMonitor> monitor) {
		monitor.when(() -> PDKInvocationMonitor.invoke(any(), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString()))
				.thenAnswer(inv -> {
					trace.add(inv.getArgument(1, PDKMethod.class).name());
					((CommonUtils.AnyError) inv.getArgument(2)).run();
					return null;
				});
	}

	@Test
	@DisplayName("生命周期顺序：INIT → 动作 → STOP，且动作拿到的就是建出来的那个节点")
	void runsInitThenActionThenStop() throws Throwable {
		try (MockedStatic<PDKInvocationMonitor> monitor = mockStatic(PDKInvocationMonitor.class);
			 MockedStatic<PDKIntegration> integration = mockStatic(PDKIntegration.class)) {
			recordLifecycle(monitor);

			String out = handler.withConnectorNode(userDb(), "orders", n -> {
				trace.add("action:" + (n == node));
				return "done";
			});

			assertEquals("done", out);
			assertEquals(Arrays.asList("INIT", "action:true", "STOP"), trace);
			// 各 handler 的 TAG 必须一路传到 PDK 调用监控，否则日志归不了因。
			monitor.verify(() -> PDKInvocationMonitor.invoke(any(), eq(PDKMethod.INIT),
					any(CommonUtils.AnyError.class), eq("probe")));
			integration.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}
	}

	@Test
	@DisplayName("动作抛错：原样上抛，但 STOP 与 releaseAssociateId 仍要跑（否则连接器句柄泄漏）")
	void whenActionThrows_stillStopsAndReleases() {
		try (MockedStatic<PDKInvocationMonitor> monitor = mockStatic(PDKInvocationMonitor.class);
			 MockedStatic<PDKIntegration> integration = mockStatic(PDKIntegration.class)) {
			recordLifecycle(monitor);
			RuntimeException boom = new RuntimeException("boom");

			RuntimeException thrown = assertThrows(RuntimeException.class,
					() -> handler.withConnectorNode(userDb(), "orders", n -> {
						throw boom;
					}));

			assertSame(boom, thrown, "动作的异常必须原样上抛——包装或吞掉都会让调用方误判");
			assertTrue(trace.contains("STOP"), "动作失败也必须 STOP");
			integration.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}
	}

	@Test
	@DisplayName("STOP 失败被吞：一次成功的动作不因收尾失败被倒打成失败")
	void whenStopFails_actionResultSurvives() throws Throwable {
		try (MockedStatic<PDKInvocationMonitor> monitor = mockStatic(PDKInvocationMonitor.class);
			 MockedStatic<PDKIntegration> integration = mockStatic(PDKIntegration.class)) {
			monitor.when(() -> PDKInvocationMonitor.invoke(any(), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString()))
					.thenAnswer(inv -> {
						String method = inv.getArgument(1, PDKMethod.class).name();
						trace.add(method);
						if ("STOP".equals(method)) {
							throw new RuntimeException("stop failed");
						}
						((CommonUtils.AnyError) inv.getArgument(2)).run();
						return null;
					});

			String out = handler.withConnectorNode(userDb(), "orders", n -> "done");

			assertEquals("done", out);
			assertTrue(trace.contains("STOP"));
			integration.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}
	}

	@Test
	@DisplayName("红线：目标解析到平台自有库 → 建节点之前就抛，绝不碰连接器（ADR-0002）")
	void redlineViolation_neverBuildsNode() throws Throwable {
		Connections evil = userDb();
		evil.setDatabase_uri(PLATFORM_URI);
		try (MockedStatic<PDKInvocationMonitor> monitor = mockStatic(PDKInvocationMonitor.class);
			 MockedStatic<PDKIntegration> integration = mockStatic(PDKIntegration.class)) {

			assertThrows(TwoDbRedlineViolationException.class,
					() -> handler.withConnectorNode(evil, "orders", n -> "done"));

			verify(handler, never()).buildNode(any(Connections.class), anyString());
			monitor.verifyNoInteractions();
			integration.verifyNoInteractions();
		}
	}

	@Test
	@DisplayName("建节点失败：associateId 仍须释放——申请了就要还")
	void whenBuildNodeFails_stillReleasesAssociateId() throws Throwable {
		try (MockedStatic<PDKInvocationMonitor> monitor = mockStatic(PDKInvocationMonitor.class);
			 MockedStatic<PDKIntegration> integration = mockStatic(PDKIntegration.class)) {
			doThrow(new IllegalStateException("no pdk jar")).when(handler)
					.buildNode(any(Connections.class), anyString());

			assertThrows(IllegalStateException.class,
					() -> handler.withConnectorNode(userDb(), "orders", n -> "done"));

			integration.verify(() -> PDKIntegration.releaseAssociateId(anyString()));
		}
	}

	@Test
	@DisplayName("连接级动作用空表映射建节点：不得按 nodeId 查 TM 的 node/tableMap（那只认任务 DAG 节点）")
	void connectionScopedTableMap_isEmptyAndDoesNotHitTm() {
		Connections conn = userDb();
		conn.setId("6a38d5da6edb30d9ee2df4f9"); // 连接 id，不是任务节点 id
		assertTrue(handler.connectionScopedTableMap(conn).isEmpty(),
				"连接级动作无任务上下文，表映射应为空；目标表由各动作显式传给 PDK");
	}
}
