package com.tapdata.tm.externalStorage.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author: sam
 * @Date: 2022/09/07
 * @Description:
 */
@Service
@Slf4j
public class ExternalStorageService extends BaseService<ExternalStorageDto, ExternalStorageEntity, ObjectId, ExternalStorageRepository> {

	public static final int DEFAULT_TTL_DAY = 3;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private TaskService taskService;

	public ExternalStorageService(@NonNull ExternalStorageRepository repository) {
		super(repository, ExternalStorageDto.class, ExternalStorageEntity.class);
	}

	protected void beforeSave(ExternalStorageDto externalStorage, UserDetail user) {
		if (StringUtils.isBlank(externalStorage.getName())) {
			throw new BizException("External.Storage.Name.Blank");
		}
		ExternalStorageDto exists = findOne(Query.query(Criteria.where("name").is(externalStorage.getName())));
		if (null != exists) {
			throw new BizException("External.Storage.Name.Exists", externalStorage.getName());
		}
		if (StringUtils.isBlank(externalStorage.getType())) {
			throw new BizException("External.Storage.Type.Blank");
		}
		ExternalStorageType externalStorageType;
		try {
			externalStorageType = ExternalStorageType.valueOf(externalStorage.getType());
		} catch (Throwable e) {
			throw new BizException("External.Storage.Type.Invalid", e, externalStorage.getType());
		}
		switch (externalStorageType) {
			case mongodb:
				if (StringUtils.isBlank(externalStorage.getUri())) {
					throw new BizException("External.Storage.MongoDB.Uri.Blank");
				}
				if (StringUtils.isBlank(externalStorage.getTable())) {
					throw new BizException("External.Storage.MongoDB.Table.Blank");
				}
				break;
			case rocksdb:
				if (StringUtils.isBlank(externalStorage.getUri())) {
					throw new BizException("External.Storage.RocksDB.Path.Blank");
				}
				break;
		}
		Integer ttlDay = externalStorage.getTtlDay();
		if (null == ttlDay || ttlDay.compareTo(0) <= 0) {
			externalStorage.setTtlDay(DEFAULT_TTL_DAY);
		}
		if (externalStorage.isDefaultStorage()) {
			update(Query.query(Criteria.where("defaultStorage").is(true)), Update.update("defaultStorage", false), user);
		}
	}

	public List<TaskDto> findUsingTasks(String id) {
		if (StringUtils.isBlank(id)) {
			return null;
		}
		Criteria esIdOrCriteria = new Criteria().orOperator(
				Criteria.where("dag.nodes.externalStorageId").is(id),
				Criteria.where("shareCDCExternalStorageId").is(id)
		);
		Criteria taskStatusCriteria = new Criteria().and("status").nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED);
		Criteria taskIsDeletedCriteria = new Criteria("is_deleted").is(false);
		Criteria criteria = new Criteria().andOperator(
				taskStatusCriteria,
				taskIsDeletedCriteria,
				esIdOrCriteria
		);
		Query query = new Query(criteria);
		query.fields().include("_id").include("name");
		List<TaskEntity> tasks = taskRepository.findAll(query);
		return CollectionUtils.isNotEmpty(tasks) ? taskService.convertToDto(tasks, TaskDto.class) : null;
	}
}