package io.tapdata.engine.it;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.verifier.ConnectorVerifier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务操控服务：包装「构造任务 → 预置 TM 侧数据 → 下发 → 等待状态/同步阶段 →
 * 端到端数据一致性断言」的完整链路，供各 IT 用例快速启动任务并验证引擎行为。
 * <p>
 * 每个方法都面向用例断言设计：{@link #startMigrateTask}/{@link #startFullSyncTask}
 * 返回 taskId，{@link #awaitStatus}/{@link #awaitSyncStage} 轮询 MockTM 中引擎上报的
 * 任务状态/进度（超时抛异常），{@link #assertDataConsistent} 经任务源/目标节点
 * connector 的旁路验证器（{@link TaskNodeVerifiers}）比对两端数据（不依赖任务自身指标，
 * 也不直连特定数据源——源/目标替换为任意 connector 时断言逻辑不变）。
 */
public class TestTaskService {

	private final EngineRuntime runtime;
	private final TaskFixture.ConnSpec source;
	private final TaskFixture.ConnSpec target;
	/** DAG 节点 id（默认 TaskDtoBuilder 两节点 DAG；自定义 DAG 时可覆盖） */
	private String sourceNodeId = TaskDtoBuilder.SOURCE_NODE_ID;
	private String targetNodeId = TaskDtoBuilder.TARGET_NODE_ID;

	public TestTaskService(EngineRuntime runtime, TaskFixture.ConnSpec source, TaskFixture.ConnSpec target) {
		this.runtime = runtime;
		this.source = source;
		this.target = target;
	}

	/** 覆盖 DAG 节点 id（自定义 DAG 结构时使用） */
	public TestTaskService nodeIds(String sourceNodeId, String targetNodeId) {
		this.sourceNodeId = sourceNodeId;
		this.targetNodeId = targetNodeId;
		return this;
	}

	// ===================== 任务下发 =====================

	/** 创建并下发迁移任务（initial_sync+cdc：全量 + 增量），返回 taskId */
	public String startMigrateTask(String name, List<String> tables) {
		return startTask(TaskDtoBuilder.buildMigrateTask(null, name, source.id, target.id, tables), tables);
	}

	/** 创建并下发全量任务（initial_sync：跑完自然 complete），返回 taskId */
	public String startFullSyncTask(String name, List<String> tables) {
		return startTask(TaskDtoBuilder.buildFullSyncTask(null, name, source.id, target.id, tables), tables);
	}

	/** 下发自定义任务（TaskFixture 预置 TM 侧数据后以 wait_run 写入 MockTM） */
	public String startTask(TaskDto taskDto, List<String> tables) {
		TaskFixture.prepare(runtime, taskDto, source, target, tables);
		return runtime.submitTask(taskDto);
	}

	/** 直接调用引擎 TaskService.startTask（绕过调度下发，用于重启续跑/调试） */
	public void startTaskDirect(TaskDto taskDto) {
		runtime.startTaskDirect(taskDto);
	}

	// ===================== 状态/阶段等待 =====================

	/** 等待任务进入指定状态集合之一 */
	public void awaitStatus(String taskId, long timeoutSeconds, String... statuses) {
		runtime.awaitTaskStatus(taskId, timeoutSeconds, statuses);
	}

	/** 等待任务进入 running */
	public void awaitRunning(String taskId, long timeoutSeconds) {
		awaitStatus(taskId, timeoutSeconds, TaskDto.STATUS_RUNNING);
	}

	/** 等待任务达到指定同步阶段（如 CDC = 全量写完进入增量） */
	public void awaitSyncStage(String taskId, String syncStage, long timeoutSeconds) {
		runtime.awaitSyncStage(taskId, syncStage, timeoutSeconds);
	}

	// ===================== 任务操控 =====================

	/** 请求停止任务（MockTM Task 置 stopping，引擎认领后停止） */
	public void stopTask(String taskId) {
		runtime.requestStopTask(taskId);
	}

	/** 读取任务文档（引擎上报到 MockTM 的最新状态） */
	public Map<String, Object> getTask(String taskId) {
		return runtime.getTask(taskId);
	}

	/** 读取任务状态字段 */
	public String getTaskStatus(String taskId) {
		return runtime.getTaskStatus(taskId);
	}

	// ===================== 数据一致性断言 =====================

	/**
	 * 旁路断言：源表与目标表（集合）逐行一致（不依赖任务自身指标）。
	 * <p>
	 * 两端数据均经任务节点 connector 的旁路验证器读取（源 = 源节点 connector 的
	 * JdbcVerifier 等，目标 = 目标节点 connector 的验证器），等待目标行数达到源行数
	 * （轮询兜底目标端写入延迟），再按主键排序、以源行列集为基准逐行比对
	 * （目标端多余列如 MongoDB _id 自动忽略）。
	 * <p>
	 * 注意：必须在任务停止/自然完成之前调用——引擎 doClose 会销毁连接器，
	 * 此后验证器不可用（连接池已关闭、节点已从注册表移除）。
	 *
	 * @param taskId      任务 id（定位源/目标节点连接器）
	 * @param table       源表名（目标表同名）
	 * @param primaryKeys 主键列（用于排序比对，默认首个键；缺省 "id"）
	 */
	public void assertDataConsistent(String taskId, String table, String... primaryKeys) {
		try {
			ConnectorVerifier sourceVerifier = TaskNodeVerifiers.awaitVerifier(taskId, sourceNodeId, toDataMap(source.config), 180);
			ConnectorVerifier targetVerifier = TaskNodeVerifiers.awaitVerifier(taskId, targetNodeId, toDataMap(target.config), 180);
			List<Map<String, Object>> sourceRows = sourceVerifier.selectAll(table);
			// 目标端写入有延迟，轮询等待行数追平（容忍单次读失败——目标写入瞬间可能有短暂不可读）
			long deadline = System.currentTimeMillis() + 120_000;
			List<Map<String, Object>> targetRows = new ArrayList<>();
			while (System.currentTimeMillis() < deadline) {
				try {
					targetRows = targetVerifier.selectAll(table);
				} catch (Exception readError) {
					sleepQuietly(500L);
					continue;
				}
				if (targetRows.size() >= sourceRows.size()) {
					break;
				}
				sleepQuietly(1000L);
			}
			compareRows(table, sourceRows, targetRows, primaryKeys);
		} catch (Exception e) {
			throw new IllegalStateException("assertDataConsistent failed: task=" + taskId + " table=" + table, e);
		}
	}

	/**
	 * 直连旁路验证器的一致性断言：验证器由调用方传入（通常为子类动态提供的
	 * 直连数据源验证器，见 {@code EngineIT.directSourceVerifier/directTargetVerifier}），
	 * 不依赖任务节点 connector 的生命周期——用于全量任务自然完成后的断言场景：
	 * 任务 complete 后引擎 doClose 已销毁任务节点连接器，只有直连验证器仍可用。
	 * <p>
	 * 验证器生命周期由调用方管理（本方法不关闭）。
	 */
	public void assertDataConsistentDirect(ConnectorVerifier sourceVerifier, ConnectorVerifier targetVerifier,
			String table, String... primaryKeys) {
		try {
			List<Map<String, Object>> sourceRows = sourceVerifier.selectAll(table);
			// 目标端写入落地可能有短暂延迟，轮询等待行数追平后比对
			long deadline = System.currentTimeMillis() + 120_000;
			List<Map<String, Object>> targetRows = new ArrayList<>();
			while (System.currentTimeMillis() < deadline) {
				targetRows = targetVerifier.selectAll(table);
				if (targetRows.size() >= sourceRows.size()) {
					break;
				}
				sleepQuietly(1000L);
			}
			compareRows(table, sourceRows, targetRows, primaryKeys);
		} catch (Exception e) {
			throw new IllegalStateException("assertDataConsistentDirect failed: table=" + table, e);
		}
	}

	/** 行数断言 + 按主键排序逐行逐列比对（目标端多余列如 MongoDB _id 自动忽略） */
	private static void compareRows(String table, List<Map<String, Object>> sourceRows,
			List<Map<String, Object>> targetRows, String... primaryKeys) {
		org.junit.jupiter.api.Assertions.assertEquals(sourceRows.size(), targetRows.size(),
				"row count mismatch: source " + table + " vs target " + table);
		String pk = primaryKeys != null && primaryKeys.length > 0 ? primaryKeys[0] : "id";
		sourceRows.sort((a, b) -> compareValues(a.get(pk), b.get(pk)));
		targetRows.sort((a, b) -> compareValues(a.get(pk), b.get(pk)));
		for (int i = 0; i < sourceRows.size(); i++) {
			Map<String, Object> expected = sourceRows.get(i);
			Map<String, Object> actual = targetRows.get(i);
			for (Map.Entry<String, Object> entry : expected.entrySet()) {
				Object expectedValue = entry.getValue();
				Object actualValue = actual.get(entry.getKey());
				org.junit.jupiter.api.Assertions.assertEquals(normalize(expectedValue), normalize(actualValue),
						"row " + i + " column " + entry.getKey() + " mismatch: source=" + expectedValue + " target=" + actualValue);
			}
		}
	}

	private static DataMap toDataMap(Map<String, Object> config) {
		return DataMap.create(config == null ? new LinkedHashMap<>() : new LinkedHashMap<>(config));
	}

	// ===================== 归一化比较 =====================

	/** 数值跨类型归一化（MySQL INT 与 MongoDB Long/Double 视为相等），null 保持 null */
	private static Object normalize(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Number) {
			double d = ((Number) value).doubleValue();
			// 整数值归一化为 long，避免 1 与 1.0 因 double 比较产生误判
			if (d == Math.floor(d) && !Double.isInfinite(d)) {
				return (long) d;
			}
			return d;
		}
		return value;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static int compareValues(Object a, Object b) {
		if (a == null && b == null) {
			return 0;
		}
		if (a == null) {
			return -1;
		}
		if (b == null) {
			return 1;
		}
		if (a instanceof Number && b instanceof Number) {
			return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
		}
		if (a instanceof Comparable && a.getClass().isInstance(b)) {
			return ((Comparable) a).compareTo(b);
		}
		return String.valueOf(a).compareTo(String.valueOf(b));
	}

	private static void sleepQuietly(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
