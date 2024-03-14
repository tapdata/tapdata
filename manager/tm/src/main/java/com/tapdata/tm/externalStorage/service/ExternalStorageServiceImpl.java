package com.tapdata.tm.externalStorage.service;

import com.mongodb.ConnectionString;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.AES256Util;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.enums.MessageType;
import com.tapdata.tm.ws.handler.TestExternalStorageHandler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author: sam
 * @Date: 2022/09/07
 * @Description:
 */
@Service
@Slf4j
public class ExternalStorageServiceImpl extends BaseService<ExternalStorageDto, ExternalStorageEntity, ObjectId, ExternalStorageRepository> implements ExternalStorageService {

	public static final int DEFAULT_TTL_DAY = 3;

	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private TaskService taskService;
	@Autowired
	private SettingsService settingsService;
	@Autowired
	private MessageQueueService messageQueueService;
	@Autowired
	private WorkerService workerService;

	public ExternalStorageServiceImpl(@NonNull ExternalStorageRepository repository) {
		super(repository, ExternalStorageDto.class, ExternalStorageEntity.class);
	}

	@Override
	public <T extends BaseDto> ExternalStorageDto save(ExternalStorageDto externalStorage, UserDetail userDetail) {
		ExternalStorageDto result;
		if (externalStorage.getId() != null) {
			Query query = new Query(Criteria.where("_id").is(externalStorage.getId()));
			this.updateByWhere(query, externalStorage, userDetail);
			result = findOne(query);
		} else {
			externalStorage.setId(null);
			externalStorage.setCanDelete(true);
			externalStorage.setCanEdit(true);
			if (!externalStorage.getType().equals(ExternalStorageType.mongodb.name())) {
				externalStorage.setStatus(DataSourceConnectionDto.STATUS_READY);
			}
			result = super.save(externalStorage, userDetail);
			if (result.getType().equals(ExternalStorageType.mongodb.name())) {
				sendTestConnection(result, userDetail);
			}
		}
		return result;
	}


	public ExternalStorageDto update(ExternalStorageDto externalStorageDto, UserDetail userDetail) {
		String type = externalStorageDto.getType();
		ExternalStorageType externalStorageType = ExternalStorageType.valueOf(type);
		if (externalStorageType == ExternalStorageType.mongodb) {
			if (StringUtils.isNotBlank(externalStorageDto.getUri())) {
				ConnectionString connectionString = new ConnectionString(externalStorageDto.getUri());
				char[] password = connectionString.getPassword();
				if (password != null && password.length != 0) {
					String pwd = new String(password);
					if (ExternalStorageDto.MASK_PWD.equals(pwd)) {
						if (null == externalStorageDto.getId()) {
							throw new BizException("External.Storage.ID.NULL");
						}
						ExternalStorageEntity oldExternalStorage = repository.findById(externalStorageDto.getId().toHexString()).orElse(null);
						if (null == oldExternalStorage) {
							return externalStorageDto;
						}
						String oldUri = oldExternalStorage.getUri();
						try {
							oldUri = AES256Util.Aes256Decode(oldUri);
						} catch (Exception ignored) {
						}
						ConnectionString oldConnectionString = new ConnectionString(oldUri);
						char[] oldPassword = oldConnectionString.getPassword();
						if (null == oldPassword || oldPassword.length == 0) {
							throw new BizException("External.Storage.MongoDB.Old.Pwd.NULL", oldUri);
						}
						String oldPasswordStr = new String(oldPassword);
						String uri = StringUtils.replaceOnce(externalStorageDto.getUri(), ExternalStorageDto.MASK_PWD, oldPasswordStr);
						externalStorageDto.setUri(uri);
					}
				}
			}
		}

		return save(externalStorageDto, userDetail);
	}

	protected void beforeSave(ExternalStorageDto externalStorage, UserDetail user) {
		if (StringUtils.isBlank(externalStorage.getName())) {
			throw new BizException("External.Storage.Name.Blank");
		}
		ExternalStorageDto exists = findOne(Query.query(Criteria.where("name").is(externalStorage.getName())));
		if (null != exists && !exists.getId().equals(externalStorage.getId())) {
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
		ExternalStorageDto oldDto = null;
		if (null != externalStorage.getId()) {
			oldDto = findById(externalStorage.getId(), user);
		}
		switch (externalStorageType) {
			case mongodb:
				if (oldDto == null) {
					if (StringUtils.isBlank(externalStorage.getUri())) {
						throw new BizException("External.Storage.MongoDB.Uri.Blank");
					}
					ConnectionString mongoUri = new ConnectionString(externalStorage.getUri());
					String database = mongoUri.getDatabase();
					if (StringUtils.isBlank(database)) {
						throw new BizException("External.Storage.MongoDB.Database.Blank");
					}
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
		Criteria esIdOrCriteria = new Criteria().orOperator(Criteria.where("dag.nodes.externalStorageId").is(id), Criteria.where("shareCDCExternalStorageId").is(id));
		Criteria taskStatusCriteria = new Criteria().and("status").nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED);
		Criteria taskIsDeletedCriteria = new Criteria("is_deleted").is(false);
		Criteria criteria = new Criteria().andOperator(taskStatusCriteria, taskIsDeletedCriteria, esIdOrCriteria);
		Query query = new Query(criteria);
		query.fields().include("_id", "name", "status", "syncType", "shareCache");
		List<TaskEntity> tasks = taskRepository.findAll(query);
		return CollectionUtils.isNotEmpty(tasks) ? taskService.convertToDto(tasks, TaskDto.class) : null;
	}

	@Override
	public Page<ExternalStorageDto> find(Filter filter, UserDetail userDetail) {
		Page<ExternalStorageDto> externalStorageDtoPage;
		if (!settingsService.isCloud()) {
			externalStorageDtoPage = super.find(filter);
		} else {
			externalStorageDtoPage = super.find(filter, userDetail);
		}
		if (null == filter.getWhere() || filter.getWhere().isEmpty() || (settingsService.isCloud())) {
			List<ExternalStorageEntity> initExternalStorages = repository.findAll(Query.query(Criteria.where("init").is(true)
					.and("status").is(DataSourceConnectionDto.STATUS_READY)));
			List<ExternalStorageDto> items = externalStorageDtoPage.getItems();
			for (ExternalStorageEntity initExternalStorage : initExternalStorages) {
				if (null == items.stream().filter(i -> i.getName().equals(initExternalStorage.getName())).findFirst().orElse(null)) {

					items.add(convertToDto(initExternalStorage, ExternalStorageDto.class));
					externalStorageDtoPage.setTotal(externalStorageDtoPage.getTotal() + 1);
				}
			}
			externalStorageDtoPage.setItems(maskPasswordIfNeed(externalStorageDtoPage.getItems()));
			externalStorageDtoPage.getItems().forEach(ets -> {
				if (null == ets.getCreateAt() && null != ets.getId()) {
					ObjectId id = ets.getId();
					int timestamp = id.getTimestamp();
					ets.setCreateAt(new Date(timestamp * 1000L));
				}
			});
		}
		return externalStorageDtoPage;
	}

	@Override
	public ExternalStorageDto findById(ObjectId objectId) {
		return maskPasswordIfNeed(super.findById(objectId));
	}

	@Override
	public ExternalStorageDto findById(ObjectId objectId, Field field, UserDetail userDetail) {
		return maskPasswordIfNeed(super.findById(objectId, field, userDetail));
	}

	@Override
	public ExternalStorageDto findById(ObjectId objectId, UserDetail userDetail) {
		return maskPasswordIfNeed(super.findById(objectId, userDetail));
	}

	@Override
	public ExternalStorageDto findById(ObjectId objectId, Field field) {
		return maskPasswordIfNeed(super.findById(objectId, field));
	}

	@Override
	public ExternalStorageDto findOne(Query query, UserDetail userDetail) {
		return maskPasswordIfNeed(super.findOne(query, userDetail));
	}

	@Override
	public ExternalStorageDto findOne(Query query) {
		return maskPasswordIfNeed(super.findOne(query));
	}

	@Override
	public ExternalStorageDto findOne(Query query, String excludeField) {
		return maskPasswordIfNeed(super.findOne(query, excludeField));
	}

	@Override
	public ExternalStorageDto findOne(Filter filter, UserDetail userDetail) {
		return maskPasswordIfNeed(super.findOne(filter, userDetail));
	}

	public ExternalStorageDto findNotCheckById(String id) {
		return super.findById(new ObjectId(id), new Field());
	}

	@Override
	public boolean deleteById(ObjectId objectId, UserDetail userDetail) {
		ExternalStorageEntity externalStorageEntity = repository.findById(objectId, userDetail).orElse(null);
		if (null == externalStorageEntity) {
			return true;
		}
		if (!externalStorageEntity.getCanDelete()) {
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

	private List<ExternalStorageDto> maskPasswordIfNeed(List<ExternalStorageDto> externalStorageDtoList) {
		if (null == externalStorageDtoList) {
			return null;
		}
		List<ExternalStorageDto> newList = new ArrayList<>();
		for (ExternalStorageDto externalStorageDto : externalStorageDtoList) {
			newList.add(maskPasswordIfNeed(externalStorageDto));
		}
		return newList;
	}

	private ExternalStorageDto maskPasswordIfNeed(ExternalStorageDto externalStorageDto) {
		if (null == externalStorageDto) {
			return null;
		}
		if (!isAgentReq()) {
			externalStorageDto.setUri(externalStorageDto.maskUriPassword());
		}
		return externalStorageDto;
	}

	@Override
	public <T extends BaseDto> T convertToDto(ExternalStorageEntity entity, Class<T> dtoClass, String... ignoreProperties) {
		T dto = super.convertToDto(entity, dtoClass, ignoreProperties);
		if (dto instanceof ExternalStorageDto) {
			ExternalStorageDto externalStorageDto = (ExternalStorageDto) dto;
			if (StringUtils.isNotBlank(externalStorageDto.getUri())) {
				try {
					externalStorageDto.setUri(AES256Util.Aes256Decode(externalStorageDto.getUri()));
				} catch (Exception ignored) {
				}
			}
		}
		return dto;
	}

	@Override
	public <T extends BaseDto> ExternalStorageEntity convertToEntity(Class<ExternalStorageEntity> externalStorageEntityClass, T dto, String... ignoreProperties) {
		ExternalStorageEntity externalStorageEntity = super.convertToEntity(externalStorageEntityClass, dto, ignoreProperties);
		if (null != externalStorageEntity) {
			if(StringUtils.isNotBlank(externalStorageEntity.getUri())){
				try {
					externalStorageEntity.setUri(AES256Util.Aes256Encode(externalStorageEntity.getUri()));
				} catch (Exception ignored) {
				}
			}
		}
		return externalStorageEntity;
	}

	public void sendTestConnection(ExternalStorageDto externalStorageDto, UserDetail user) {
		log.info("Send external storage test connection, external storage = {}", externalStorageDto.getName());

		List<Worker> availableAgent = workerService.findAvailableAgent(user);
		if (org.apache.commons.collections4.CollectionUtils.isEmpty(availableAgent)) {
			log.warn("Send external storage test connection failed, agent not found");
			return;
		}

		String processId = availableAgent.get(0).getProcessId();
		Map<String, Object> data = new HashMap<>();
		data.put("id", externalStorageDto.getId().toHexString());
		MessageQueueDto queueDto = new MessageQueueDto();

		TestExternalStorageHandler testExternalStorageHandler = SpringContextHelper.getBean(TestExternalStorageHandler.class);
		if (null == testExternalStorageHandler) {
			return;
		}
		MessageInfo messageInfo = testExternalStorageHandler.wrapMessageInfo(user, data, MessageType.TEST_CONNECTION);
		if (null == messageInfo) {
			return;
		}
		BeanUtils.copyProperties(messageInfo, queueDto);
		if (!(queueDto.getData() instanceof Map)) {
			return;
		}
		((Map)queueDto.getData()).put("editTest", false);
		((Map)queueDto.getData()).put("type", MessageType.TEST_CONNECTION.getType());
		queueDto.setReceiver(processId);
		queueDto.setType("pipe");

		log.info("Build send test connection websocket context, processId = {}, userId = {}", processId, user.getUserId());
		messageQueueService.sendMessage(queueDto);

		Update update = Update.update("status", "testing").set("testTime", System.currentTimeMillis());
		update(new Query(Criteria.where("_id").is(externalStorageDto.getId())), update, user);
	}
}
