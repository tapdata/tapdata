package com.tapdata.tm.externalStorage.service;

import com.mongodb.ConnectionString;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
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
				ConnectionString mongoUri = new ConnectionString(externalStorage.getUri());
				String database = mongoUri.getDatabase();
				if (StringUtils.isBlank(database)) {
					throw new BizException("External.Storage.MongoDB.Database.Blank");
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
		query.fields().include("_id", "name", "status", "syncType");
		List<TaskEntity> tasks = taskRepository.findAll(query);
		return CollectionUtils.isNotEmpty(tasks) ? taskService.convertToDto(tasks, TaskDto.class) : null;
	}

	@Override
	public Page<ExternalStorageDto> find(Filter filter, UserDetail userDetail) {
		Page<ExternalStorageDto> externalStorageDtoPage = super.find(filter, userDetail);
		if (null == filter.getWhere() || filter.getWhere().isEmpty()) {
			List<ExternalStorageEntity> initExternalStorages = repository.findAll(Query.query(Criteria.where("init").is(true)));
			List<ExternalStorageDto> items = externalStorageDtoPage.getItems();
			for (ExternalStorageEntity initExternalStorage : initExternalStorages) {
				if (null == items.stream().filter(i -> i.getName().equals(initExternalStorage.getName())).findFirst().orElse(null)) {
					items.add(convertToDto(initExternalStorage, ExternalStorageDto.class));
					externalStorageDtoPage.setTotal(externalStorageDtoPage.getTotal() + 1);
				}
			}
		}
		return externalStorageDtoPage;
	}

	@Override
	public boolean deleteById(ObjectId objectId, UserDetail userDetail) {
		ExternalStorageEntity externalStorageEntity = repository.findById(objectId, userDetail).orElse(null);
		if (null == externalStorageEntity) {
			return true;
		}
		if (!externalStorageEntity.isCanDelete()) {
			return true;
		}
		boolean delete = super.deleteById(objectId, userDetail);
		if (externalStorageEntity.isDefaultStorage()) {
			Query query = Query.query(Criteria.where("type").ne(ExternalStorageType.memory.name()));
			ExternalStorageEntity findExternalStorage = repository.findOne(query, userDetail).orElse(null);
			if (null == findExternalStorage) {
				findExternalStorage = repository.findOne(new Query(), userDetail).orElse(null);
			}
			if (null != findExternalStorage) {
				query = Query.query(Criteria.where("_id").is(findExternalStorage.getId()));
				Update update = new Update().set("defaultStorage", true);
				repository.updateFirst(query, update, userDetail);
			}
		}
		return delete;
	}
}