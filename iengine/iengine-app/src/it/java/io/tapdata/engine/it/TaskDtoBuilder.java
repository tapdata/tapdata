package io.tapdata.engine.it;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 任务构造器：构造最小可运行任务（TaskDto），供各 IT 用例经
 * {@link EngineRuntime#submitTask(TaskDto)} 下发到 MockTM 由引擎调度执行。
 * <p>
 * 与 {@link TaskFixture} 配合使用：TaskDtoBuilder 只负责任务配置本身
 * （DAG 节点/边/同步类型），TaskFixture 负责预置任务运行所需的 TM 侧数据
 * （Connections/DatabaseTypes/transformAllParam 模型）。
 * <p>
 * 节点装配说明（与 TM 端保存的任务一致）：
 * <ul>
 *   <li>源节点：DatabaseNode，connectionId=源连接，tableNames=待同步表（库级迁移）</li>
 *   <li>目标节点：DatabaseNode，connectionId=目标连接</li>
 *   <li>边：source → target；DAG 由 {@link DAG#build(com.tapdata.tm.commons.task.dto.Dag)} 构建，
 *       序列化时经 TaskDto.dag 的 {@code @JsonSerialize(DagSerialize)} 还原为 {nodes, edges} 结构</li>
 * </ul>
 */
public class TaskDtoBuilder {

	public static final String SOURCE_NODE_ID = "source-db-node";
	public static final String TARGET_NODE_ID = "target-db-node";

	private TaskDtoBuilder() {
	}

	/**
	 * 构造库级迁移任务（initial_sync+cdc，源库指定表 → 目标库）。
	 *
	 * @param taskIdHex   任务 _id（24 位 hex），可空自动生成
	 * @param name        任务名
	 * @param sourceConnId 源连接 id（Connections._id）
	 * @param targetConnId 目标连接 id（Connections._id）
	 * @param tables      待同步表名列表
	 */
	public static TaskDto buildMigrateTask(String taskIdHex, String name, String sourceConnId, String targetConnId, List<String> tables) {
		TaskDto taskDto = baseTask(taskIdHex, name, TaskDto.SYNC_TYPE_MIGRATE);
		taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC_CDC);
		taskDto.setSourceId(sourceConnId);
		taskDto.setSourceName(sourceConnId);
		taskDto.setTargetId(targetConnId);
		taskDto.setTargetName(targetConnId);

		DatabaseNode source = new DatabaseNode();
		source.setId(SOURCE_NODE_ID);
		source.setName("source-db");
		source.setConnectionId(sourceConnId);
		source.setTableNames(tables == null ? new ArrayList<>() : new ArrayList<>(tables));

		DatabaseNode target = new DatabaseNode();
		target.setId(TARGET_NODE_ID);
		target.setName("target-db");
		target.setConnectionId(targetConnId);

		setDag(taskDto, source, target);
		return taskDto;
	}

	/**
	 * 构造全量同步任务（initial_sync，跑完自然 complete，不进入增量）。
	 */
	public static TaskDto buildFullSyncTask(String taskIdHex, String name, String sourceConnId, String targetConnId, List<String> tables) {
		TaskDto taskDto = baseTask(taskIdHex, name, TaskDto.SYNC_TYPE_MIGRATE);
		taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC);
		taskDto.setSourceId(sourceConnId);
		taskDto.setSourceName(sourceConnId);
		taskDto.setTargetId(targetConnId);
		taskDto.setTargetName(targetConnId);

		DatabaseNode source = new DatabaseNode();
		source.setId(SOURCE_NODE_ID);
		source.setName("source-db");
		source.setConnectionId(sourceConnId);
		source.setTableNames(tables == null ? new ArrayList<>() : new ArrayList<>(tables));

		DatabaseNode target = new DatabaseNode();
		target.setId(TARGET_NODE_ID);
		target.setName("target-db");
		target.setConnectionId(targetConnId);

		setDag(taskDto, source, target);
		return taskDto;
	}

	/** 基础字段：id/name/syncType/status/userId */
	private static TaskDto baseTask(String taskIdHex, String name, String syncType) {
		TaskDto taskDto = new TaskDto();
		taskDto.setId(StringUtils.isBlank(taskIdHex) ? new ObjectId() : new ObjectId(taskIdHex));
		taskDto.setName(name);
		taskDto.setSyncType(syncType);
		taskDto.setStatus(TaskDto.STATUS_WAIT_RUN);
		taskDto.setUserId(EngineRuntime.DEFAULT_USER_ID);
		// 引擎 HazelcastPdkBaseNode.generateNodeConfig 会把 fileLog/doubleActive/dataSaving/
		// oldVersionTimezone 塞进 nodeConfig（任务为 null 时 put null），connector 侧
		// BeanMap.put 会覆盖其默认值并直接 unboxing 判空 NPE，必须显式给出非 null 值
		taskDto.setFileLog(false);
		taskDto.setDoubleActive(false);
		taskDto.setDataSaving(false);
		taskDto.setOldVersionTimezone(false);
		// 引擎 HazelcastSourcePdkBaseNode.initStreamOffsetInitialAndCDC 直接 unboxing
		// getShareCdcEnable()，null 会 NPE；false 走普通 CDC（timestampToStreamOffset）
		taskDto.setShareCdcEnable(false);
		return taskDto;
	}

	private static void setDag(TaskDto taskDto, DatabaseNode source, DatabaseNode target) {
		com.tapdata.tm.commons.task.dto.Dag dagDto = new com.tapdata.tm.commons.task.dto.Dag();
		dagDto.setNodes(Arrays.asList(source, target));
		dagDto.setEdges(Collections.singletonList(new Edge(source.getId(), target.getId())));
		taskDto.setDag(DAG.build(dagDto));
	}
}
