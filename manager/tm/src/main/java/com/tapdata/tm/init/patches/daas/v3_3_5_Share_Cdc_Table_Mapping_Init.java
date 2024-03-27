package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import io.tapdata.utils.AppType;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.shareCdcTableMapping.service.ShareCdcTableMappingService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-07-06 17:33
 **/
@PatchAnnotation(appType = AppType.DAAS, version = "3.3-5")
public class v3_3_5_Share_Cdc_Table_Mapping_Init extends AbsPatch {
	private static final Logger logger = LogManager.getLogger(v3_3_5_Share_Cdc_Table_Mapping_Init.class);
	public static final String TAG = v3_3_5_Share_Cdc_Table_Mapping_Init.class.getSimpleName();

	public v3_3_5_Share_Cdc_Table_Mapping_Init(PatchType type, PatchVersion version) {
		super(type, version);
	}

	@Override
	public void run() {
		ShareCdcTableMappingRepository shareCdcTableMappingRepository = SpringContextHelper.getBean(ShareCdcTableMappingRepository.class);
		MongoTemplate mongoTemplate = shareCdcTableMappingRepository.getMongoOperations();

		DataSourceRepository dataSourceRepository = SpringContextHelper.getBean(DataSourceRepository.class);

		// Create Index
		Index signIndex = new CompoundIndexDefinition(new Document("sign", 1)).unique().background();
		logger.info("[{}] Create share cdc table mapping sign index: {}", TAG, signIndex);
		mongoTemplate.indexOps(ShareCdcTableMappingEntity.class).ensureIndex(signIndex);
		Index index = new CompoundIndexDefinition(new Document("connectionId", 1).append("tableName", 1).append("shareCdcTaskId", 1)).background();
		logger.info("[{}] Create share cdc table mapping index: {}", TAG, index);
		mongoTemplate.indexOps(ShareCdcTableMappingEntity.class).ensureIndex(index);

		TaskRepository taskRepository = SpringContextHelper.getBean(TaskRepository.class);
		Query taskQuery = new Query(Criteria.where("syncType").is(TaskDto.SYNC_TYPE_LOG_COLLECTOR).and("is_deleted").is(false));
		taskQuery.fields().include("_id", "name", "dag");
		List<TaskEntity> logCollectorTasks = taskRepository.findAll(taskQuery);
		logger.info("[{}] Create share cdc table mapping for log collector tasks: {}", TAG, logCollectorTasks.size());
		if (CollectionUtils.isNotEmpty(logCollectorTasks)) {
			long startMS = System.currentTimeMillis();
			int taskCounter = 0;
			for (TaskEntity logCollectorTask : logCollectorTasks) {
				DAG dag = logCollectorTask.getDag();
				String taskId = logCollectorTask.getId().toHexString();
				String taskName = logCollectorTask.getName();
				if (null == dag) {
					continue;
				}
				List<Node> nodes = dag.getNodes();
				if (CollectionUtils.isEmpty(nodes)) {
					continue;
				}
				Node foundNode = nodes.stream().filter(n -> n instanceof LogCollectorNode).findFirst().orElse(null);
				if (null == foundNode) {
					continue;
				}
				LogCollectorNode logCollectorNode = (LogCollectorNode) foundNode;
				List<String> tableNames = logCollectorNode.getTableNames();
				Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
				Map<String, List<String>> connTableNames = new HashMap<>();
				if (null != logCollectorConnConfigs) {
					for (Map.Entry<String, LogCollecotrConnConfig> entry : logCollectorConnConfigs.entrySet()) {
						String connectionId = entry.getKey();
						LogCollecotrConnConfig logCollecotrConnConfig = entry.getValue();
						List<String> connConfigTableNames = logCollecotrConnConfig.getTableNames();
						connTableNames.put(connectionId, connConfigTableNames);
					}
				} else if (CollectionUtils.isNotEmpty(tableNames)) {
					connTableNames.put(logCollectorNode.getConnectionIds().get(0), tableNames);
				}
				bulkUpsertShareCdcTableMapping(connTableNames, taskId, taskName, shareCdcTableMappingRepository, dataSourceRepository);
				taskCounter++;
				if (taskCounter % 10 == 0) {
					logger.info("[{}] Create share cdc table mapping for log collector tasks: {}/{}", TAG, taskCounter, logCollectorTasks.size());
				}
			}
			long costMS = System.currentTimeMillis() - startMS;
			logger.info("[{}] Create share cdc table mapping finish, cost: {} ms", TAG, costMS);
		}
	}

	private void bulkUpsertShareCdcTableMapping(Map<String, List<String>> connTableNames, String taskId, String taskName,
												ShareCdcTableMappingRepository shareCdcTableMappingRepository,
												DataSourceRepository dataSourceRepository) {
		if (MapUtils.isEmpty(connTableNames)) {
			return;
		}
		MongoTemplate mongoTemplate = shareCdcTableMappingRepository.getMongoOperations();
		BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ShareCdcTableMappingEntity.class);
		List<Pair<Query, Update>> queryUpdateList = new ArrayList<>();
		for (Map.Entry<String, List<String>> entry : connTableNames.entrySet()) {
			String connId = entry.getKey();
			List<String> tableNames = entry.getValue();
			if (logger.isDebugEnabled()) {
				logger.debug("[{}] Create share cdc table mapping for log collector task: {}({}), connection id: {}, table names: {}", TAG, taskName, taskId, connId, tableNames);
			}
			String connNamespaceStr = dataSourceRepository.findById(connId)
					.map(DataSourceEntity::getNamespace)
					.map(ns -> String.join(".", ns)).orElse(null);
			for (String tableName : tableNames) {
				ShareCdcTableMappingEntity shareCdcTableMappingEntity = new ShareCdcTableMappingEntity();
				shareCdcTableMappingEntity.setExternalStorageTableName(ShareCdcTableMappingService.SHARE_CDC_KEY_PREFIX + taskName + "_" + String.join(".", connNamespaceStr, tableName));
				shareCdcTableMappingEntity.setVersion("v1");
				shareCdcTableMappingEntity.setShareCdcTaskId(taskId);
				shareCdcTableMappingEntity.setTableName(tableName);
				shareCdcTableMappingEntity.setConnectionId(connId);
				shareCdcTableMappingEntity.setSign(shareCdcTableMappingEntity.genSign());
				Query query = new Query(Criteria.where("sign").is(shareCdcTableMappingEntity.getSign()));
				Update update = shareCdcTableMappingRepository.buildUpdateSet(shareCdcTableMappingEntity);
				queryUpdateList.add(Pair.of(query, update));
				if (queryUpdateList.size() >= 1000) {
					bulkOperations.upsert(queryUpdateList);
					bulkOperations.execute();
					queryUpdateList.clear();
					bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ShareCdcTableMappingEntity.class);
				}
			}
		}
		if (CollectionUtils.isNotEmpty(queryUpdateList)) {
			bulkOperations.upsert(queryUpdateList);
			bulkOperations.execute();
		}
	}
}
