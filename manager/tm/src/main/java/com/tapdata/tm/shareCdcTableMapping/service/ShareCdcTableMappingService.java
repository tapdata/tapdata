package com.tapdata.tm.shareCdcTableMapping.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.task.service.TaskService;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author:
 * @Date: 2023/10/16
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ShareCdcTableMappingService extends BaseService<ShareCdcTableMappingDto, ShareCdcTableMappingEntity, ObjectId, ShareCdcTableMappingRepository> {

	public final static String SHARE_CDC_KEY_PREFIX = "ExternalStorage_SHARE_CDC_";

	private TaskService taskService;
	private DataSourceService dataSourceService;

	public ShareCdcTableMappingService(@NonNull ShareCdcTableMappingRepository repository) {
		super(repository, ShareCdcTableMappingDto.class, ShareCdcTableMappingEntity.class);
	}

	protected void beforeSave(ShareCdcTableMappingDto shareCdcTableMapping, UserDetail user) {

	}

	public void genShareCdcTableMappingsByLogCollectorTask(TaskDto logCollectorTask, boolean newTask,UserDetail user) {
		if (null == logCollectorTask) return;
		DAG dag = logCollectorTask.getDag();
		if (null == dag) return;
		TaskDto existsLogCollectorTask = null;
		if (!newTask) {
			existsLogCollectorTask = taskService.findByTaskId(logCollectorTask.getId(), "dag");
		}
		Map<String, List<String>> connId2TableNames = getConnId2TableNames(logCollectorTask);
		if (MapUtils.isEmpty(connId2TableNames)) return;
		Map<String, List<String>> existsConnId2TableNames = getConnId2TableNames(existsLogCollectorTask);
		List<ShareCdcTableMappingEntity> wait2SaveShareCdcTableMappingEntities = new ArrayList<>();
		BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
		for (Map.Entry<String, List<String>> entry : connId2TableNames.entrySet()) {
			String connId = entry.getKey();
			List<String> tableNames = entry.getValue();
			List<String> existsTableNames = existsConnId2TableNames.get(connId);
			List<String> addedTableNames = new ArrayList<>(tableNames);
			if (CollectionUtils.isNotEmpty(existsTableNames)) {
				addedTableNames.removeAll(existsTableNames);
			}
			if (CollectionUtils.isEmpty(addedTableNames)) {
				continue;
			}
			String connNamespaceStr = Optional.ofNullable(dataSourceService.findById(new ObjectId(connId)))
					.map(DataSourceConnectionDto::getNamespace)
					.map(ns -> String.join(".", ns)).orElse(null);
			for (String addedTableName : addedTableNames) {
				ShareCdcTableMappingEntity shareCdcTableMappingEntity = new ShareCdcTableMappingEntity();
				shareCdcTableMappingEntity.setConnectionId(connId);
				shareCdcTableMappingEntity.setTableName(addedTableName);
				shareCdcTableMappingEntity.setShareCdcTaskId(logCollectorTask.getId().toHexString());
				shareCdcTableMappingEntity.setSign(shareCdcTableMappingEntity.genSign());
				Query query = Query.query(Criteria.where("sign").is(shareCdcTableMappingEntity.getSign()));
				Optional<ShareCdcTableMappingEntity> existsTableMapping = repository.findOne(query);
				if (existsTableMapping.isPresent()) {
					continue;
				}
				shareCdcTableMappingEntity.setVersion(ShareCdcTableMappingDto.VERSION_V2);
				shareCdcTableMappingEntity.setExternalStorageTableName(genExternalStorageTableName(connId, connNamespaceStr, addedTableName));
				wait2SaveShareCdcTableMappingEntities.add(shareCdcTableMappingEntity);
			}
			if (wait2SaveShareCdcTableMappingEntities.size() >= 1000) {
				bulkUpsert(wait2SaveShareCdcTableMappingEntities, bulkOperations,user);
				bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
				wait2SaveShareCdcTableMappingEntities.clear();
			}
		}
		if (CollectionUtils.isNotEmpty(wait2SaveShareCdcTableMappingEntities)) {
			bulkUpsert(wait2SaveShareCdcTableMappingEntities, bulkOperations, user);
		}
	}

	public static String genExternalStorageTableName(String connId, String connNamespaceStr, String tableName) {
		String name = SHARE_CDC_KEY_PREFIX;
		if (null != connNamespaceStr) {
			name += String.join("_", connId, String.join(".", connNamespaceStr, tableName)).hashCode();
		} else {
			name += String.join("_", connId, tableName).hashCode();
		}
		return name;
	}

	private void bulkUpsert(List<ShareCdcTableMappingEntity> wait2SaveShareCdcTableMappingEntities, BulkOperations bulkOperations,UserDetail user) {
		for (ShareCdcTableMappingEntity wait2SaveShareCdcTableMappingEntity : wait2SaveShareCdcTableMappingEntities) {
			Query query = Query.query(Criteria.where("sign").is(wait2SaveShareCdcTableMappingEntity.getSign()));
			Update update = repository.buildUpdateSet(wait2SaveShareCdcTableMappingEntity,user);
			bulkOperations.upsert(query, update);
		}
		bulkOperations.execute();
	}

	public Map<String, List<String>> getConnId2TableNames(TaskDto taskDto) {
		Map<String, List<String>> connId2TableNames = new HashMap<>();
		if (null == taskDto) return connId2TableNames;
		DAG dag = taskDto.getDag();
		if (null == dag) return connId2TableNames;
		Node foundNode = dag.getNodes().stream().filter(n -> n instanceof LogCollectorNode).findFirst().orElse(null);
		if (null == foundNode) return connId2TableNames;
		LogCollectorNode logCollectorNode = (LogCollectorNode) foundNode;
		Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
		if (MapUtils.isNotEmpty(logCollectorConnConfigs)) {
			for (Map.Entry<String, LogCollecotrConnConfig> entry : logCollectorConnConfigs.entrySet()) {
				String connId = entry.getKey();
				LogCollecotrConnConfig logCollecotrConnConfig = entry.getValue();
				List<String> tableNames = logCollecotrConnConfig.getTableNames();
				connId2TableNames.put(connId, tableNames);
			}
		} else {
			List<String> tableNames = logCollectorNode.getTableNames();
			String connId = logCollectorNode.getConnectionIds().get(0);
			connId2TableNames.put(connId, tableNames);
		}
		return connId2TableNames;
	}
}