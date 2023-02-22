package io.tapdata.milestone;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Milestone;
import com.tapdata.entity.dataflow.SyncObjects;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2021-07-27 19:13
 **/
public class MilestoneJetEdgeService extends MilestoneService {

	private static List<MilestoneStage> hiddenStages;
	private MilestoneContext taskMilestoneContext;
	private ExecutorService executorService;
	private ClientMongoOperator clientMongoOperator;

	public MilestoneJetEdgeService(MilestoneContext milestoneContext, MilestoneContext taskMilestoneContext) {
		super(milestoneContext);
		this.taskMilestoneContext = taskMilestoneContext;
		this.executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r->new Thread(r, "Milestone-Jet-Edge-Runner"));
		this.clientMongoOperator = buildOperator();
	}

	static {
		hiddenStages = new ArrayList<>();
		hiddenStages.add(MilestoneStage.INIT_DATAFLOW);
		hiddenStages.add(MilestoneStage.CONNECT_TO_SOURCE);
		hiddenStages.add(MilestoneStage.CONNECT_TO_TARGET);
		hiddenStages.add(MilestoneStage.CREATE_TARGET_INDEX);
		hiddenStages.add(MilestoneStage.READ_SOURCE_DDL);
		hiddenStages.add(MilestoneStage.CREATE_TARGET_VIEW);
		hiddenStages.add(MilestoneStage.CREATE_TARGET_PROCEDURE);
		hiddenStages.add(MilestoneStage.CREATE_TARGET_FUNCTION);
	}

	@Override
	public List<Milestone> initMilestones() {
		if (null == this.milestoneContext.getTaskDto() || null == this.milestoneContext.getSourceNode() || null == this.milestoneContext.getDestNode()) {
			throw new IllegalArgumentException("Milestone context missing task, source node, target node");
		}

		String srcConnectionId = null;
		if (milestoneContext.getSourceNode() != null && milestoneContext.getSourceNode() instanceof DataParentNode) {
			srcConnectionId = ((DataParentNode<?>) milestoneContext.getSourceNode()).getConnectionId();
		}
		if (StringUtils.isNotBlank(srcConnectionId)) {
			Query query = new Query(Criteria.where("_id").is(srcConnectionId));
			query.fields().exclude("schema");
			milestoneContext.setSourceConn(MongodbUtil.getConnections(query, clientMongoOperator, true));
		}

		String tgtConnectionId = null;
		if (milestoneContext.getDestNode() != null && milestoneContext.getDestNode() instanceof DataParentNode) {
			tgtConnectionId = ((DataParentNode<?>) milestoneContext.getDestNode()).getConnectionId();
		}

		if (StringUtils.isNotBlank(tgtConnectionId)) {
			Query query = new Query(Criteria.where("_id").is(tgtConnectionId));
			query.fields().exclude("schema");
			this.milestoneContext.setTargetConn(MongodbUtil.getConnections(query, clientMongoOperator, true));
		} else if (milestoneContext.getDestNode() instanceof CacheNode) {
			this.milestoneContext.setTargetConn(Connections.cacheConnection(milestoneContext.getSourceConn(), HazelcastUtil.node2Stages(milestoneContext.getDestNode())));
		}

		List<Milestone> milestoneList;
//    if (taskMilestoneContext.getEdgeMilestones() == null) {
//      milestoneContext.setEdgeMilestones(new ConcurrentHashMap<>());
//    }
		String edgeKey = MilestoneUtil.getEdgeKey(this.milestoneContext.getSourceVertexName(), this.milestoneContext.getDestVertexName());
		if (MapUtils.isEmpty(taskMilestoneContext.getEdgeMilestones()) || !taskMilestoneContext.getEdgeMilestones().containsKey(edgeKey) || CollectionUtils.isEmpty(taskMilestoneContext.getEdgeMilestones().get(edgeKey).getMilestones())) {
			// 如果任务不存在里程碑列表(第一次运行，旧任务)，则根据任务配置，生成里程碑列表
			milestoneList = generateMilestonesByEdge();
//      taskMilestoneContext.getEdgeMilestones().put(edgeKey, new EdgeMilestone(milestoneContext.getSourceVertexName(), milestoneContext.getDestVertexName(), milestoneList));
		} else {
			// 如果任务已经存在里程碑，修改状态
			milestoneList = taskMilestoneContext.getEdgeMilestones().get(edgeKey).getMilestones();

			for (Milestone milestone : milestoneList) {

				if (MilestoneStage.valueOf(milestone.getCode()).isNeedOffsetEmpty()) {
					// 如果已经不是增量阶段，则保持一部分里程碑的状态不变，这些里程碑只有在初始化的时候运行
					continue;
				}

				milestone.setStatus(MilestoneStatus.WAITING.getStatus());
				milestone.setStart(System.currentTimeMillis());
				milestone.setEnd(0L);
			}
		}
		return milestoneList;
	}

	private List<Milestone> generateMilestonesByEdge() {
		MilestoneStage[] allMilestoneStages = MilestoneStage.values();
		TaskDto taskDto = milestoneContext.getTaskDto();
		String syncType = taskDto.getType();
		String mappingTemplate;
		if ("migrate".equals(taskDto.getSyncType())) {
			mappingTemplate = ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE;
		} else {
			mappingTemplate = ConnectorConstant.MAPPING_TEMPLATE_CUSTOM;
		}
		DatabaseTypeEnum sourceType = DatabaseTypeEnum.UNKNOWN;
		if (null != milestoneContext.getSourceConn() && null != milestoneContext.getSourceConn().getDatabase_type()) {
			try {
				sourceType = DatabaseTypeEnum.fromString(milestoneContext.getSourceConn().getDatabase_type());
			} catch (Exception ignored) {
			}
		}
		DatabaseTypeEnum targetType = DatabaseTypeEnum.UNKNOWN;
		if (null != milestoneContext.getTargetConn() && null != milestoneContext.getTargetConn().getDatabase_type()) {
			try {
				targetType = DatabaseTypeEnum.fromString(milestoneContext.getTargetConn().getDatabase_type());
			} catch (Exception ignored) {
			}
		}

		List<Milestone> milestones = new ArrayList<>();

		for (MilestoneStage milestoneStage : allMilestoneStages) {

			if (hiddenStages.contains(milestoneStage)) {
				continue;
			}

			if (milestoneStage.isNeedSameSourceAndTarget() && !sourceType.equals(targetType)) {
				continue;
			}

			DatabaseTypeEnum[] sourceDatabases = milestoneStage.getSourceDatabases();
			if (CollectionUtils.isNotEmpty(Arrays.asList(sourceDatabases))) {
				if (milestoneStage.isSourceTypeInclude() && arrayNotInclude(sourceDatabases, sourceType)) {
					continue;
				} else if (!milestoneStage.isSourceTypeInclude() && arrayInclude(sourceDatabases, sourceType)) {
					continue;
				}
			}

			DatabaseTypeEnum[] targetDatabases = milestoneStage.getTargetDatabases();
			if (CollectionUtils.isNotEmpty(Arrays.asList(targetDatabases)) && arrayNotInclude(targetDatabases, targetType)) {
				continue;
			}

			String[] syncTypes = milestoneStage.getSyncTypes();
			if (CollectionUtils.isNotEmpty(Arrays.asList(syncTypes)) && arrayNotInclude(syncTypes, syncType)) {
				continue;
			}

			String[] mappingTemplates = milestoneStage.getMappingTemplates();
			if (CollectionUtils.isNotEmpty(Arrays.asList(mappingTemplates)) && arrayNotInclude(mappingTemplates, mappingTemplate)) {
				continue;
			}

			boolean needOffsetEmpty = milestoneStage.isNeedOffsetEmpty();
			if (needOffsetEmpty && null != taskDto.getAttrs() && null != taskDto.getAttrs().get("syncProgress")) {
				continue;
			}

			// 特殊适配，使用custom sql方式进行增量
			if (!customSqlCDCPredicate(milestoneStage, syncType)) {
				continue;
			}

			// 特殊适配，是否需要drop schema
			if (keepSchemaPredicate(milestoneStage)) {
				continue;
			}

			// 特殊适配，是否需要清空目标数据
			if (!clearDataPredicate(milestoneStage)) {
				continue;
			}

			// 特殊适配，是否需要有创建表、视图、函数、存储过程的里程碑
			if (!checkDDLMilestone(milestoneStage)) {
				continue;
			}

			if ((milestoneStage.equals(MilestoneStage.CLEAR_TARGET_DATA) || milestoneStage.equals(MilestoneStage.READ_SOURCE_DDL)) && ConnectorConstant.SYNC_TYPE_CDC.equals(syncType)) {
				continue;
			}

			if (milestoneStage.equals(MilestoneStage.CREATE_TARGET_INDEX) && !this.milestoneContext.getTaskDto().getIsAutoCreateIndex()) {
				continue;
			}

			milestones.add(new Milestone(milestoneStage.name(), MilestoneStatus.WAITING.getStatus(), System.currentTimeMillis(), milestoneStage.getGroup().getName()));
		}

		return milestones;
	}

	/**
	 * 检查是否是custom sql的方式进行增量，如果是，需要添加CDC的里程碑节点
	 *
	 * @param milestoneStage
	 * @return
	 */
	private boolean customSqlCDCPredicate(MilestoneStage milestoneStage, String syncType) {
		if ((milestoneStage.equals(MilestoneStage.READ_CDC_EVENT) || milestoneStage.equals(MilestoneStage.WRITE_CDC_EVENT)) && syncType.equals(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC)) {

//      boolean increment = milestoneContext.getDataFlow().getSetting().getIncrement();
//      if (!increment) {
//        return false;
//      }

			if (!checkCustomSqlNeedCdc()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * 遍历mappings，检查是否有至少一个custom sql并且包含"${OFFSET1}"
	 *
	 * @return
	 */
	private boolean checkCustomSqlNeedCdc() {
		Node sourceNode = milestoneContext.getSourceNode();
		if (sourceNode instanceof TableNode) {
			final String increasesql = ((TableNode) sourceNode).getIncreasesql();
			return StringUtils.isNotBlank(increasesql) && increasesql.contains("${OFFSET1}");
		}
		return false;
	}

	private boolean keepSchemaPredicate(MilestoneStage milestoneStage) {
		if (milestoneStage.equals(MilestoneStage.DROP_TARGET_SCHEMA)) {
			Node destNode = milestoneContext.getDestNode();
			if (destNode instanceof TableNode) {
				final Boolean dropTable = ((TableNode) destNode).getDropTable();
				return null != dropTable && dropTable;
			} else if (destNode instanceof DatabaseNode) {
				final String dropType = ((DatabaseNode) destNode).getExistDataProcessMode();
				return !"dropTable".equals(dropType);
			}
		}

		return false;
	}

	private boolean clearDataPredicate(MilestoneStage milestoneStage) {
		if (milestoneStage.equals(MilestoneStage.CLEAR_TARGET_DATA)) {
			Node destNode = milestoneContext.getDestNode();
			if (destNode instanceof TableNode) {
				final Boolean dropTable = ((TableNode) destNode).getDropTable();
				return null != dropTable && dropTable;
			} else if (destNode instanceof DatabaseNode) {
				final String dropType = ((DatabaseNode) destNode).getExistDataProcessMode();
				StringUtils.equalsAny(dropType, "removeData", "dropTable");
			}
		}

		return true;
	}

	private boolean checkDDLMilestone(MilestoneStage milestoneStage) {
		List<SyncObjects> syncObjects = null;
		if (milestoneContext.getSourceNode() instanceof DatabaseNode) {
			final List<com.tapdata.tm.commons.dag.vo.SyncObjects> taskSyncObjects = ((DatabaseNode) milestoneContext.getSourceNode()).getSyncObjects();
			if (CollectionUtils.isNotEmpty(taskSyncObjects)) {
				syncObjects = new ArrayList<>();
				for (com.tapdata.tm.commons.dag.vo.SyncObjects syncObject : taskSyncObjects) {
					syncObjects.add(new SyncObjects(syncObject.getType(), syncObject.getObjectNames()));
				}
			}
		}
		boolean result = true;
		if (CollectionUtils.isNotEmpty(syncObjects)) {
			switch (milestoneStage) {
				case CREATE_TARGET_TABLE:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.TABLE_TYPE)) == null) {
						result = false;
					}

					break;
				case CREATE_TARGET_VIEW:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.VIEW_TYPE)) == null) {
						result = false;
					}

					break;
				case CREATE_TARGET_FUNCTION:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.FUNCTION_TYPE)) == null) {
						result = false;
					}

					break;
				case CREATE_TARGET_PROCEDURE:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.PROCEDURE_TYPE)) == null) {
						result = false;
					}

					break;
				default:
					break;
			}
		}

		return result;
	}

	@Override
	public void updateList() {
		Query query = new Query(Criteria.where("_id").is(milestoneContext.getTaskDto().getId().toHexString()));
		final Map<String, EdgeMilestone> edgeMilestones = taskMilestoneContext.getEdgeMilestones();
		Update update = new Update().set("attrs.edgeMilestones", edgeMilestones);
		clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
	}

	@Override
	public void updateList(List<Milestone> milestoneList) {
		String edgeKey = MilestoneUtil.getEdgeKey(this.milestoneContext.getSourceVertexName(), this.milestoneContext.getDestVertexName());
		Query query = new Query(Criteria.where("_id").is(milestoneContext.getTaskDto().getId().toHexString()));
		Update update = new Update().set("attrs.edgeMilestones." + edgeKey, new EdgeMilestone(
				this.milestoneContext.getSourceVertexName(), this.milestoneContext.getDestVertexName(),
				milestoneList
		));
		clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
	}

	@Override
	public void updateMilestoneStatusByCode(MilestoneStage milestoneStage, MilestoneStatus milestoneStatus, String errorMessage) {
		this.executorService.submit(() -> {
			List<String> vertexNames;
			MilestoneContext.VertexType vertexType;
			if (CollectionUtils.isEmpty(this.milestoneContext.getSourceVertexNames()) && CollectionUtils.isNotEmpty(this.milestoneContext.getDestVertexNames())) {
				vertexNames = this.milestoneContext.getDestVertexNames();
				vertexType = MilestoneContext.VertexType.SOURCE;
			} else if (CollectionUtils.isNotEmpty(this.milestoneContext.getSourceVertexNames()) && CollectionUtils.isEmpty(this.milestoneContext.getDestVertexNames())) {
				vertexNames = this.milestoneContext.getSourceVertexNames();
				vertexType = MilestoneContext.VertexType.DEST;
			} else {
				throw new IllegalArgumentException("Source vertex names and desc vertex names cannot both empty");
			}
			String collection = ConnectorConstant.TASK_COLLECTION;
			for (String vertexName : vertexNames) {
				String edgeKey;
				switch (vertexType) {
					case SOURCE:
						edgeKey = MilestoneUtil.getEdgeKey(this.milestoneContext.getSourceVertexName(), vertexName);
						break;
					case DEST:
						edgeKey = MilestoneUtil.getEdgeKey(vertexName, this.milestoneContext.getDestVertexName());
						break;
					default:
						edgeKey = "";
						break;
				}
				String key = "attrs.edgeMilestones." + edgeKey + "." + MILESTONES_FIELD_NAME;
				Query query = new Query(Criteria.where("_id").is(milestoneContext.getTaskDto().getId()).and(key).elemMatch(Criteria.where("code").is(milestoneStage.name()).and("status").ne(MilestoneStatus.ERROR.getStatus())));
				Update update = new Update().set(key + ".$.status", milestoneStatus.getStatus());
				// 开始时间
				if (milestoneStatus.equals(MilestoneStatus.RUNNING)) {
					update.set(key + ".$.start", System.currentTimeMillis());
				}
				// 结束时间
				if (milestoneStatus.equals(MilestoneStatus.FINISH) || milestoneStatus.equals(MilestoneStatus.ERROR)) {
					update.set(key + ".$.end", System.currentTimeMillis());
				}
				// 错误描述
				if (milestoneStatus.equals(MilestoneStatus.ERROR) && StringUtils.isNotBlank(errorMessage)) {
					update.set(key + ".$.errorMessage", errorMessage);
				}
				clientMongoOperator.update(query, update, collection);
			}
		});
	}
}
