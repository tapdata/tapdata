package com.tapdata.tm.ds.service.impl;

import cn.hutool.core.lang.Assert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.classification.dto.ClassificationDto;
import com.tapdata.tm.classification.service.ClassificationService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.ds.dto.ConnectionStats;
import com.tapdata.tm.ds.dto.UpdateTagsDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.ds.utils.UriRootConvertUtils;
import com.tapdata.tm.ds.vo.SupportListVo;
import com.tapdata.tm.ds.vo.ValidateTableVo;
import com.tapdata.tm.job.dto.JobDto;
import com.tapdata.tm.job.service.JobService;
import com.tapdata.tm.libSupported.entity.LibSupportedsEntity;
import com.tapdata.tm.libSupported.repository.LibSupportedsRepository;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.proxy.dto.SubscribeResponseDto;
import com.tapdata.tm.proxy.service.impl.ProxyService;
import com.tapdata.tm.task.service.LogCollectorService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.typemappings.service.TypeMappingsService;
import com.tapdata.tm.utils.*;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@Service
@Slf4j
//@Setter(onMethod_ = {@Autowired})
public class DataSourceService extends BaseService<DataSourceConnectionDto, DataSourceEntity, ObjectId, DataSourceRepository> {

	private final static String connectNameReg = "^([\u4e00-\u9fa5]|[A-Za-z])[\\s\\S]*$";
	@Value("${gateway.secret:}")
	private String gatewaySecret;
	@Value("#{'${spring.profiles.include:idaas}'.split(',')}")
	private List<String> productList;
	@Autowired
	private SettingsService settingsService;
	private final Object checkCloudLock = new Object();
	@Autowired
	private ClassificationService classificationService;
	@Autowired
	private MetadataInstancesService metadataInstancesService;
	@Autowired
	private WorkerService workerService;
	@Autowired
	private MetadataUtil metadataUtil;
	@Autowired
	private JobService jobService;
	@Autowired
	private DataFlowService dataFlowService;
	@Autowired
	private TaskService taskService;
	@Autowired
	private MessageQueueService messageQueueService;
	@Autowired
	private ModulesService modulesService;
	@Autowired
	private LibSupportedsRepository libSupportedsRepository;
	@Autowired
	private DataSourceDefinitionService dataSourceDefinitionService;
	@Autowired
	private DefaultDataDirectoryService defaultDataDirectoryService;
	@Autowired
	private AlarmService alarmService;
	@Autowired
	private TypeMappingsService typeMappingsService;

	@Autowired
	@Lazy
	private LogCollectorService logCollectorService;

	public DataSourceService(@NonNull DataSourceRepository repository) {
		super(repository, DataSourceConnectionDto.class, DataSourceEntity.class);
	}
	public DataSourceConnectionDto add(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
		Boolean submit = connectionDto.getSubmit();
		connectionDto.setLastUpdAt(new Date());
		connectionDto = save(connectionDto, userDetail);

		desensitizeMongoConnection(connectionDto);
		sendTestConnection(connectionDto, false, submit, userDetail);
		defaultDataDirectoryService.addConnection(connectionDto, userDetail);
		return connectionDto;
	}
	public DataSourceConnectionDto addWithSpecifiedId(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
		Boolean submit = connectionDto.getSubmit();
		connectionDto.setLastUpdAt(new Date());

		beforeSave(connectionDto, userDetail);

		repository.insert(convertToEntity(DataSourceEntity.class, connectionDto), userDetail);

		connectionDto = findById(connectionDto.getId(), userDetail);

		desensitizeMongoConnection(connectionDto);
		sendTestConnection(connectionDto, false, submit, userDetail);
		return connectionDto;
	}

	private void checkRepeatName(UserDetail user, String name, ObjectId id) {
		//检查是否存在名称相同的数据源连接，存在的话，则不允许。（还可以检验一下关键字）
		if (StringUtils.isNotBlank(name) && checkRepeatNameBool(user, name, id)) {
			log.warn("data source connections name is already exist, name = {}", name);
			throw new BizException("Datasource.RepeatName", "Data source connections name is already exist");
		}
	}

	private boolean checkRepeatNameBool(UserDetail user, String name, ObjectId id) {
		log.debug("check connection repeat name, name = {}, id = {}, user = {}", name, id, user == null ? null : user.getUserId());
		Criteria criteria = Criteria.where("name").is(name);
		if (id != null) {
			criteria.and("_id").ne(id);
		}
		Query query = Query.query(criteria);
		long count = repository.count(query, user);
		return count > 0;
	}


	/**
	 * 测试连接  和编辑数据源的时候，都调用方法
	 *
	 * @param user
	 * @param updateDto
	 * @return
	 */
	public DataSourceConnectionDto update(UserDetail user, DataSourceConnectionDto updateDto, boolean changeLast) {
		Boolean submit = updateDto.getSubmit();
		String oldName = updateCheck(user, updateDto);

		Assert.isFalse(StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), updateDto.getAccessNodeType())
				&& CollectionUtils.isEmpty(updateDto.getAccessNodeProcessIdList()), "manually_specified_by_the_user processId is null");

		ObjectId id = updateDto.getId();
		DataSourceConnectionDto oldConnection = null;
		if (Objects.nonNull(id)) {
			oldConnection = findById(id);
			if (DataSourceConnectionDto.STATUS_TESTING.equals(updateDto.getStatus()) && !DataSourceConnectionDto.STATUS_TESTING.equals(oldConnection.getStatus())) {
				updateDto.setLastStatus(oldConnection.getStatus());
			}
		}
		Map<String, Object> config = updateDto.getConfig();
		if (oldConnection != null) {

			if (((updateDto.getShareCdcEnable() != null && updateDto.getShareCdcEnable())
					|| (oldConnection.getShareCdcEnable() != null && oldConnection.getShareCdcEnable()))
					&& updateDto.getShareCDCExternalStorageId() != null && !updateDto.getShareCDCExternalStorageId().equals(oldConnection.getShareCDCExternalStorageId())) {
				//查询当前数据源存在的运行中的任务，存在则不允许修改，给出报错。
				Boolean canUpdate = logCollectorService.checkUpdateConfig(updateDto.getId().toHexString(), user);
				if (!canUpdate) {
					throw new BizException("LogCollect.ExternalStorageUpdateError");
				}
			}
			if (Objects.nonNull(config)) {
				Map<String, Object> dataConfig = oldConnection.getConfig();
				if (dataConfig.containsKey("password") && !config.containsKey("password")) {
					config.put("password", dataConfig.get("password"));
				} else if (dataConfig.containsKey("mqPassword") && !config.containsKey("mqPassword")) {
					config.put("mqPassword", dataConfig.get("mqPassword"));
				}
			}
		}

		DataSourceEntity entity = convertToEntity(DataSourceEntity.class, updateDto);
		entity.setAccessNodeProcessIdList(updateDto.getTrueAccessNodeProcessIdList());

		Update update = repository.buildUpdateSet(entity, user);

		if (StringUtils.equals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), updateDto.getAccessNodeType())) {
			update.set("accessNodeProcessId", null);
			update.set("accessNodeProcessIdList", Lists.of());
		}

		if (updateDto.getLoadAllTables() != null && updateDto.getLoadAllTables()) {
			update.set("table_filter", null);
		}

		if (changeLast) {
			updateById(updateDto.getId(), update, user);
		} else {
			updateByIdNotChangeLast(updateDto.getId(), update, user);
		}

		updateDto = findById(updateDto.getId(), user);

		updateAfter(user, updateDto, oldName, submit);

		hiddenMqPasswd(updateDto);

		return updateDto;
	}

	//返回oldName, 表示更换名称
	public String updateCheck(UserDetail user, DataSourceConnectionDto updateDto) {
		Assert.isFalse(StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), updateDto.getAccessNodeType())
				&& CollectionUtils.isEmpty(updateDto.getAccessNodeProcessIdList()), "manually_specified_by_the_user processId is null");

		//校验数据源的名称是否合法
		checkName(updateDto.getName());
		checkRepeatName(user, updateDto.getName(), updateDto.getId());
		DataSourceConnectionDto connectionDto = findById(updateDto.getId(), user);
		if (connectionDto == null) {
			throw new BizException("Datasource.NotFound", "Data source connections not found or not belong to current user");
		}

		// should encode the password even if the username not exist
		if (StringUtils.isNotBlank(updateDto.getPlain_password())) {
			restoreAccessNodeType(updateDto, connectionDto);
		}

		checkAccessNodeAvailable(updateDto.getAccessNodeType(), updateDto.getAccessNodeProcessIdList(), user);

		if ((StringUtils.isNotBlank(connectionDto.getDatabase_username()) || StringUtils.isNotBlank(updateDto.getDatabase_username()))
				&& StringUtils.isNotBlank(updateDto.getPlain_password())) {
			updateDto.setDatabase_password(AES256Util.Aes256Encode(updateDto.getPlain_password()));
		}

		String oldName = null;
		if (!connectionDto.getName().equals(updateDto.getName())) {
			oldName = connectionDto.getName();
		}

		updateDto.setTestTime(System.currentTimeMillis());

		//设置agentTags
		setAgentTag(updateDto);

		String uri = patchDataFilter(updateDto, connectionDto.getDatabase_password());

		updateDto.setPlain_password("");
		updateDto.setDatabase_uri(uri);


		Map<String, Object> config = updateDto.getConfig();
		if (config != null) {
			Object password = config.get("password");
			if (password == null || StringUtils.isBlank((String) password)) {
				if (StringUtils.isNotBlank((String) connectionDto.getConfig().get("password"))) {
					config.put("password", connectionDto.getConfig().get("password"));
				}
			}

			Object mqPassword = config.get("mqPassword");
			if (mqPassword == null || StringUtils.isBlank((String) mqPassword)) {
				if (StringUtils.isNotBlank((String) connectionDto.getConfig().get("password"))) {
					config.put("mqPassword", connectionDto.getConfig().get("mqPassword"));
				}
			}

			if (updateDto.getDatabase_type().toLowerCase(Locale.ROOT).contains("mongo") && config.get("uri") != null) {
				String uri1 = (String) config.get("uri");
				if (StringUtils.isNotBlank(uri1) && uri1.contains("******")) {
					ConnectionString uri2 = new ConnectionString((String) connectionDto.getConfig().get("uri"));
					if (uri2.getPassword() != null) {
						String password1 = new String(uri2.getPassword());
						uri1 = uri1.replace("******", password1);
						config.put("uri", uri1);
					} else {
						config.put("uri", connectionDto.getConfig().get("uri"));
					}
				}
			}
		}

//        校验数据源定义的，暂时可以先不管
//        if (updateDto.getConfig() != null) {
//            DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(connectionDto.getDatabaseType(), user);
//            if (definitionDto == null) {
//                throw new BizException("Data source definition not found");
//            }
//
//            boolean ok = JsonSchemaUtils.checkDataSourceDefinition(definitionDto, updateDto.getConfig());
//            if (!ok) {
//                throw new BizException("Data source connections config is invalid");
//            }
//        }
		return oldName;
	}

	/**
	 * check flow engine available
	 *
	 * @param accessNodeType          accessNodeType
	 * @param accessNodeProcessIdList accessNodeProcessIdList
	 * @param userDetail              user
	 */
	public void checkAccessNodeAvailable(String accessNodeType, List<String> accessNodeProcessIdList, UserDetail userDetail) {
		//todo 这个接口应该移动到workService
		if (StringUtils.equals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), accessNodeType) ||
				CollectionUtils.isEmpty(accessNodeProcessIdList)) {
			return;
		}

		List<Worker> availableAgentByAccessNode = workerService.findAvailableAgentByAccessNode(userDetail, accessNodeProcessIdList);

		if (CollectionUtils.isEmpty(availableAgentByAccessNode)) {
			throw new BizException("Datasource.AgentNotFound", "Data source flow engine not found or not belong to current user");
		}

	}

	/**
	 * 加载schema 前端入参accessNodeType没有数据 反序列化会给默认值，保存的时候会覆盖原数据
	 *
	 * @param updateDto     更新数据
	 * @param connectionDto 原数据
	 */
	private void restoreAccessNodeType(DataSourceConnectionDto updateDto, DataSourceConnectionDto connectionDto) {
		if (updateDto.isAccessNodeTypeEmpty() && CollectionUtils.isNotEmpty(connectionDto.getAccessNodeProcessIdList())) {
			updateDto.setAccessNodeType(connectionDto.getAccessNodeType());
			updateDto.setAccessNodeProcessId(connectionDto.getAccessNodeProcessIdList().get(0));
			updateDto.setAccessNodeProcessIdList(connectionDto.getAccessNodeProcessIdList());
			updateDto.setAccessNodeTypeEmpty(false);
		}
	}


	private String patchDataFilter(DataSourceConnectionDto updateDto, String databasePassword) {
		log.debug("build mongo uri, update dto = {}, databasePassword = {}", updateDto, databasePassword);
		String result = null;
		if (DataSourceEnum.isMongoDB(updateDto.getDatabase_type()) || DataSourceEnum.isGridFs(updateDto.getDatabase_type())) {

			if (StringUtils.isNotBlank(updateDto.getDatabase_username()) && StringUtils.isBlank(updateDto.getPlain_password())) {
				updateDto.setDatabase_password(databasePassword);
			}
			result = UriRootConvertUtils.constructUri(updateDto);
		}

		log.debug("result = {}", result);
		return result;
	}


	private void checkUser(UserDetail user, String id) {
		Where where = Where.where("_id", toObjectId(id));
		long count = count(where, user);
		if (count < 1) {
			throw new BizException("Datasource.NotFound", "Data source connections not found or not belong to current user");
		}
	}


	public Page<DataSourceConnectionDto> list(Filter filter, boolean noSchema, UserDetail userDetail) {
		Page<DataSourceConnectionDto> dataSourceConnectionDtoPage = find(filter, userDetail);
		List<DataSourceConnectionDto> items = dataSourceConnectionDtoPage.getItems();

		Map<ObjectId, DataSourceConnectionDto> newResultObj = buildFindResult(noSchema, items, userDetail);
		items = items.stream().map(i -> newResultObj.get(i.getId())).collect(Collectors.toList());

		dataSourceConnectionDtoPage.setItems(items);

		return dataSourceConnectionDtoPage;
	}

	public DataSourceConnectionDto getById(ObjectId objectId, com.tapdata.tm.base.dto.Field fields, Boolean noSchema, UserDetail user) {
		DataSourceConnectionDto connectionDto = findById(objectId, fields, user);
		if (Objects.isNull(connectionDto)) {
			return null;
		}

		if (!Objects.isNull(noSchema)) {
			List<DataSourceConnectionDto> items = new ArrayList<>();
			items.add(connectionDto);
			Map<ObjectId, DataSourceConnectionDto> map = buildFindResult(noSchema, items, user);
			return map.get(connectionDto.getId());
		} else {
			return connectionDto;
		}

	}

	private Map<ObjectId, DataSourceConnectionDto> buildFindResult(boolean noSchema, List<DataSourceConnectionDto> items, UserDetail user) {
		Map<String, DataSourceConnectionDto> connectMap = new HashMap<>();
		Map<ObjectId, DataSourceConnectionDto> newResultObj = new HashMap<>();

		Set<String> databaseTypes = items.stream().map(DataSourceConnectionDto::getDatabase_type).collect(Collectors.toSet());
		List<DataSourceDefinitionDto> definitionDtoList = dataSourceDefinitionService.getByDataSourceType(new ArrayList<>(databaseTypes), user);
		Map<String, DataSourceDefinitionDto> definitionMap = definitionDtoList.stream().collect(Collectors.toMap(DataSourceDefinitionDto::getPdkHash, Function.identity(), (f1, f2) -> f1));

		for (DataSourceConnectionDto item : items) {
			if (!isAgentReq()) {
				if ((DataSourceEnum.isMongoDB(item.getDatabase_type()) || DataSourceEnum.isGridFs(item.getDatabase_type()))
						&& StringUtils.isNotBlank(item.getDatabase_uri())) {
					item.setDatabase_uri(UriRootConvertUtils.hidePassword(item.getDatabase_uri()));
				}
			}

			//不需要这个操作了。引擎会更新这个东西，另外每次更新databasetypes的时候，需要更新这个  参考： updateCapabilities方法
            if (definitionMap.containsKey(item.getDatabase_type())) {
				DataSourceDefinitionDto definitionDto = definitionMap.get(item.getPdkHash());
				item.setCapabilities(definitionDto.getCapabilities());
				item.setDefinitionPdkId(definitionDto.getPdkId());
				item.setPdkType(definitionDto.getPdkType());
				item.setPdkHash(definitionDto.getPdkHash());
				item.setDefinitionPdkId(definitionDto.getPdkId());
				item.setDefinitionGroup(definitionDto.getGroup());
				item.setDefinitionVersion(definitionDto.getVersion());
				item.setDefinitionScope(definitionDto.getScope());
				item.setDefinitionBuildNumber(String.valueOf(definitionDto.getBuildNumber()));
				item.setDefinitionTags(definitionDto.getTags());
            }

			desensitizeMongoConnection(item);

			hiddenMqPasswd(item);

			// 访问节点 默认值平台指定
			if (StringUtils.isEmpty(item.getAccessNodeType())) {
				item.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
			}

			// --！
			int loadCount = Objects.nonNull(item.getLoadCount()) ? item.getLoadCount() : 0;
			int tableCount = Objects.nonNull(item.getTableCount()) ? item.getTableCount().intValue() : 0;
			if (loadCount > tableCount) {
				item.setLoadCount(tableCount);
			}


			String id = item.getId().toHexString();
			connectMap.put(id, item);
			newResultObj.put(item.getId(), item);
		}
		Set<ObjectId> connectionIdList = newResultObj.keySet();
		if (CollectionUtils.isNotEmpty(connectionIdList) && !noSchema) {
			List<String> metaTypes = Lists.of("database", "apiendpoint", "directory", "ftp");
			Criteria criteria = Criteria.where("source.id").in(connectionIdList).and("metaType").in(metaTypes);
			List<MetadataInstancesDto> instancesDtos = metadataInstancesService.findAllDto(new Query(criteria), user);
			if (CollectionUtils.isNotEmpty(instancesDtos)) {
				Map<String, MetadataInstancesDto> databaseObjs = instancesDtos.stream()
						.collect(Collectors.toMap(m -> m.getId().toHexString(), Function.identity()));

				Criteria criteria1 = Criteria.where("databaseId").in(databaseObjs.keySet())
						.and("is_deleted").is(false)
						.and("metaType").in(Lists.of("collection", "table", "file"));
				Query query = new Query(criteria1);
				query.fields().include("databaseId", "fields", "original_name", "meta_type", "id", "indices", "columnPosition", "source._id", "partitionSet");
				List<MetadataInstancesDto> collections = metadataInstancesService.findAllDto(query, user);

				if (CollectionUtils.isNotEmpty(collections)) {
					Map<String, List<MetadataInstancesDto>> collectionObjs = collections.stream()
							.collect(Collectors.groupingBy(MetadataInstancesDto::getDatabaseId));

					if (!collectionObjs.isEmpty()) {
						collectionObjs.forEach((databaseId, list) -> {
							if (CollectionUtils.isNotEmpty(list)) {
								Schema oldSchema = SchemaTransformUtils.newSchema2oldSchema(list);
								if (oldSchema != null && CollectionUtils.isNotEmpty(oldSchema.getTables())) {
									List<Table> tables = oldSchema.getTables();
									for (Table table : tables) {
										if (CollectionUtils.isNotEmpty(table.getFields())) {
											List<Field> fields = table.getFields().stream().filter(f -> !f.isDeleted()).collect(Collectors.toList());
											table.setFields(fields);
										}
									}

									oldSchema.setTables(tables);

									String sourceId = databaseObjs.get(databaseId).getSource().getId().toHexString();
									DataSourceConnectionDto newConn = connectMap.get(sourceId);
									connectMap.remove(sourceId);
									if (newConn != null) {
										newConn.setSchema(oldSchema);
										//newResult.add(newConn);
										newResultObj.put(newConn.getId(), newConn);
									}
								}
							}
						});

						connectMap.forEach((k, v) -> newResultObj.put(MongoUtils.toObjectId(k), v));
					}
				}
			}
		}
		return newResultObj;
	}

	private void hiddenMqPasswd(DataSourceConnectionDto item) {
		if (item != null && !isAgentReq() && !Objects.isNull(item.getConfig())
				&& !item.getConfig().isEmpty()) {
			if (item.getConfig().containsKey("password")) {
				item.getConfig().put("password", null);
			}
			if (item.getConfig().containsKey("mqPassword")) {
				item.getConfig().put("mqPassword", null);
			}


			if (item.getDatabase_type().toLowerCase(Locale.ROOT).contains("mongo") && item.getConfig().get("uri") != null) {
				String uri = (String) item.getConfig().get("uri");
				ConnectionString connectionString = null;
				try {
					connectionString = new ConnectionString(uri);
				} catch (Exception e) {
					if (uri.startsWith("mongodb+srv:")) {
						try {
							connectionString = new ConnectionString(uri.replace("mongodb+srv:", "mongodb:"));
						} catch (Exception e1) {
							log.error("Parse connection string failed ({}) {}", uri, e.getMessage());
						}
					} else {
						log.error("Parse connection string failed ({}) {}", uri, e.getMessage());
					}
				}

				if (connectionString != null) {
					char[] password = connectionString.getPassword();

					if (password != null) {
						String password1 = new String(password);
						uri = uri.replace(":"+password1, ":******");
						item.getConfig().put("uri", uri);
					}

				}

			}
		}
	}


	/**
	 * 给数据源连接修改资源分类
	 *
	 * @param user
	 * @param updateTagsDto 跟新tag列表
	 * @return
	 */

	@Deprecated
	@Transactional
	public void updateTag(UserDetail user, UpdateTagsDto updateTagsDto) {
		//遍历校验分类是否存在
		if (updateTagsDto == null) {
			return;
		}

		if (CollectionUtils.isNotEmpty(updateTagsDto.getListtags())) {
			checkTagList(user, updateTagsDto.getListtags());
		}

		//根据用户id遍历校验所有的数据源连接是否存在，如果不存在
		//将数据源连接设置上所有的标签
		if (CollectionUtils.isNotEmpty(updateTagsDto.getId())) {
			List<String> id = updateTagsDto.getId();
			List<ObjectId> ids = id.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
			Query query = new Query(Criteria.where("_id").in(ids));
			Update update = Update.update("tagList", updateTagsDto.getListtags());
			update(query, update);

			//更新成功后，需要将模型中的也跟着更新了
			Criteria criteria = Criteria.where("source.id").in(ids).and("metaType").is("database").and("isDeleted").is(false);
			Update classifications = Update.update("classifications", updateTagsDto.getListtags());
			metadataInstancesService.update(new Query(criteria), classifications, user);
		}
	}

	@Deprecated
	private void checkTagList(UserDetail user, List<Tag> tags) {
		log.debug("check classification, tags = {}, user = {}", tags, user == null ? null : user.getUserId());
		for (Tag tag : tags) {
			ClassificationDto classificationDto = classificationService.findById(MongoUtils.toObjectId(tag.getId()), user);
			if (classificationDto == null) {
				throw new BizException("Tag.NotFound", "resource tag not found");
			}
		}
	}


	/**
	 * 删除数据源连接
	 *
	 * @param user
	 * @param id
	 * @return
	 */
	public DataSourceConnectionDto delete(UserDetail user, String id) {
		//根据用户id与数据源连接id检验当前数据源连接是否存在
		DataSourceConnectionDto connectionDto = findById(toObjectId(id), user);
		if (connectionDto == null) {
			throw new BizException("Datasource.NotFound", "connections not found or not belong to current user");
		}

		// 如果有心跳任务，先停止后删除
		taskService.deleteHeartbeatByConnId(user, id);

		//根据数据源id查询所有的jobModel, ModulesModel, dataFlowsModel， 如果存在，则不允许删除connection
		//将数据源连接删除

		log.debug("query data source related jobs");
		Criteria jobCriteria1 = Criteria.where("connections.source").is(toObjectId(id));
		Criteria jobCriteria2 = Criteria.where("connections.target").is(toObjectId(id));
		Criteria jobCriteria3 = new Criteria().orOperator(jobCriteria1, jobCriteria2);
		Query jobQuery = new Query(jobCriteria3);
		jobQuery.fields().include("_id", "name", "connections", "status", "user_id");
		List<JobDto> jobDtos = jobService.findAll(jobQuery);

		log.debug("query data source related dataflow");
		Criteria regex = Criteria.where("stages.connectionId").is(id);
		Query dataflowQuery = new Query(regex);
		dataflowQuery.fields().include("_id", "name");
		List<DataFlowDto> dataFlowDtos = dataFlowService.findAll(dataflowQuery);

		if (CollectionUtils.isNotEmpty(jobDtos) || CollectionUtils.isNotEmpty(dataFlowDtos)) {
			log.info("the connection referenced by other jobs, jobs = {}, dataflows = {}", jobDtos.size(), dataFlowDtos.size());
			throw new BizException("Datasource.LinkJobs");
		}

		log.debug("query data source related task");
		Criteria taskCriteria = Criteria.where("is_deleted").is(false).and("status").ne("delete_failed").orOperator(Criteria.where("dag.nodes.connectionId").is(id), Criteria.where("dag.nodes.connectionIds").in(id));
		Query taskQuery = new Query(taskCriteria);
		taskQuery.fields().include("_id", "name");
		List<TaskDto> allDto = taskService.findAllDto(taskQuery, user);

		if (CollectionUtils.isNotEmpty(allDto)) {
			log.info("the connection referenced by other jobs, tasks = {}", allDto.size());
			throw new BizException("Datasource.LinkJobs");
		}

		//校验module关联数据
		List<ModulesDto> releatedModules = modulesService.findByConnectionId(id);
		if (CollectionUtils.isNotEmpty(releatedModules)) {
			log.info("the connection referenced by other modules  ");
			throw new BizException("Datasource.LinkModules");
		}

		deleteById(toObjectId(id), user);

		//删除数据源相关的模型
		log.debug("delete data source related model");
		Criteria criteria = Criteria.where("source._id").is(id);
		Query query = new Query(criteria);
		query.fields().include("id", "name", "original_name");
		MetadataInstancesDto one = metadataInstancesService.findOne(query, user);
		if (one != null) {
			Query query1 = new Query(Criteria.where("databaseId").is(one.getId().toHexString()));
			Update update = Update.update("is_deleted", true).unset("relation").unset("lienage").unset("fields_lienage").unset("fields").unset("indexes");
			metadataInstancesService.update(query1, update, user);

			Query query2 = new Query(Criteria.where("id").is(one.getId()));
			Update update2 = Update.update("is_deleted", true);
			metadataInstancesService.update(query2, update2, user);
		}

		defaultDataDirectoryService.removeConnection(id, user);

		return connectionDto;
	}


	/**
	 * 复制数据源
	 *
	 * @param user
	 * @param id
	 * @param requestURI
	 * @return
	 */
	public DataSourceConnectionDto copy(UserDetail user, String id, String requestURI) {
		boolean boolValue = SettingsEnum.CONNECTIONS_CREAT_DUPLICATE_SOURCE.getBoolValue(true);
		log.debug("system duplicateCreate param = {}", boolValue);
		if (!boolValue) {
			log.warn("duplicate source");
			throw new BizException("Datasource.DuplicateSource", "duplicate source");
		}

		//根据用户与数据源连接id查询数据源连接
		Optional<DataSourceEntity> optional = repository.findById(toObjectId(id), user);
		if (!optional.isPresent()) {
			log.warn("Data source connection not found");
			throw new BizException("Datasource.NotFound", "data source connection not found");
		}
		//json schemal
		DataSourceEntity entity = optional.get();
		entity.setId(null);
		//将数据源连接的名称修改成为名称后面+_copy
		String connectionName = entity.getName() + " - Copy";
		entity.setLastUpdAt(new Date());
		entity.setStatus("testing");

		while (true) {
			try {
				//插入复制的数据源
				entity.setName(connectionName);
				entity.setStatus(DataSourceEntity.STATUS_INVALID);
				entity.setLoadFieldsStatus(null);
				entity.setLoadCount(0);
				entity.setLoadFieldsStatus("");
				entity = repository.save(entity, user);
				break;
			} catch (Exception e) {
				if (e.getMessage().contains("duplicate key error")) {
					connectionName = connectionName + " - Copy";
				} else {
					throw e;
				}
			}
		}
		this.resetWebHookOnCopy(entity,user, requestURI);//重置WebHook URL

		log.debug("copy datasource success, datasource name = {}", connectionName);

		//将新的数据源连接返回
		DataSourceConnectionDto connectionDto = convertToDto(entity, DataSourceConnectionDto.class);
		sendTestConnection(connectionDto, true, true, user);
		defaultDataDirectoryService.addConnection(connectionDto, user);
		return connectionDto;
	}

	/**
	 * 复制数据源时，重置WebHook连接（For SaaS）
	 * 在数据源JSONSchema中对connection配置了actionOnCopy属性
	 *
	 * @param entity
	 * @param user
	 * @param requestURI
	 * @Author Gavin
	 * @Date 2022-10-17
	 */
	private void resetWebHookOnCopy(DataSourceEntity entity, UserDetail user, String requestURI){
			ObjectId copyId = entity.getId();
			if (null == copyId) entity.setId(copyId = new ObjectId());
			//获取并校验pdkHash,用于获取jsonSchema
			String pdkHash = entity.getPdkHash();
			if (null == pdkHash || "".equals(pdkHash)) {
				log.debug("Reset WebHook URL error, datasource name = {}, message : PdkHash must be not null or not empty.", entity.getName());
				return;
			}
			DataSourceDefinitionDto dataSource = dataSourceDefinitionService.findByPdkHash(pdkHash, Integer.MAX_VALUE, user);
			LinkedHashMap<String, Object> properties = dataSource.getProperties();
			//获取connection配置
			if (null == properties || properties.isEmpty()) {
				log.debug("Reset WebHook URL error, datasource name = {}, message : Connector's jsonSchema must be not null or not empty.", entity.getName());
				return;
			}
			Object connection = properties.get("connection");
			if (null == connection || !(connection instanceof Map) || ((Map<String, Object>) connection).isEmpty()) {
				log.debug("Reset WebHook URL error, datasource name = {}, message : Connector's connection must be not null or not empty.",entity.getName());
				return;
			}
			//获取properties配置信息
			Object connectionPropertiesObj = ((Map<String, Object>) connection).get("properties");
			if (null == connectionPropertiesObj || !(connectionPropertiesObj instanceof Map)) {
				log.debug("Reset WebHook URL error, datasource name = {}, message : Connector's properties must be not null or not empty.",entity.getName());
				return;
			}
			Map<String, Object> connectionProperties = (Map<String, Object>) connectionPropertiesObj;
			String entityId = copyId.toHexString();
			//遍历properties属性，对含有actionOnCopy属性的字段进行重置WebHook操作
			for (Map.Entry<String, Object> entry : connectionProperties.entrySet()) {
				String key = entry.getKey();
				Object propertiesObj = entry.getValue();
				if (null == propertiesObj || !(propertiesObj instanceof Map) || ((Map) propertiesObj).isEmpty()) continue;
				Object actionOnCopyObj = ((Map<String, Object>) propertiesObj).get("actionOnCopy");
				if (null == actionOnCopyObj) continue;
				//对actionOnCopy值为NEW_WEB_HOOK_URL的属性进行WebHook重置操作，使用connection Id生成特有的WebHook url
				if ("NEW_WEB_HOOK_URL".equals(String.valueOf(actionOnCopyObj))) {
					Map<String, Object> config = entity.getConfig();
					Object keyObjOfCopyConnectionConfig = config.get(key);
					if (null == keyObjOfCopyConnectionConfig) continue;
					String keyValue = String.valueOf(keyObjOfCopyConnectionConfig);
					URL url = null;
					try {
						url = new URL(keyValue);
					} catch (Throwable ignored) {
					}
					if (null == keyValue || !keyValue.contains("/api/proxy/callback/") || url == null) {
						config.put(key, "");
					} else {
						int lastCharIndex = keyValue.lastIndexOf('/') + 1;
//						int lenOfToken = keyValue.length();
						SubscribeDto subscribeDto = new SubscribeDto();
						subscribeDto.setExpireSeconds(Integer.MAX_VALUE);
						subscribeDto.setSubscribeId("source#" + entityId);

						ProxyService proxyService = InstanceFactory.bean(ProxyService.class);

						String token = null;
						if(productList != null && productList.contains("dfs")) {
							if(!StringUtils.isBlank(gatewaySecret))
								token = proxyService.generateStaticToken(user.getUserId(), gatewaySecret);
							else
								throw new BizException("gatewaySecret can not be read from @Value(\"${gateway.secret}\")");
						}
						SubscribeResponseDto subscribeResponseDto = proxyService.generateSubscriptionToken(subscribeDto, user, token, requestURI);
						String webHookUrl = keyValue.substring(0, lastCharIndex) + subscribeResponseDto.getToken();
						config.put(key, webHookUrl);

						repository.update(new Query(Criteria.where("_id").is(entityId)),entity);
					}
				}
			}
	}
	/**
	 * mongodb这种类型数据源类型的，存在库里面的就是一个uri,需要解析成为一个标准模式的返回
	 *
	 * @param id
	 * @param tableName
	 * @param schema
	 * @param user
	 * @return
	 */
	public DataSourceConnectionDto customQuery(ObjectId id, String tableName, Boolean schema, UserDetail user) {
		Criteria criteriaOne = Criteria.where("id").is(id);
		DataSourceConnectionDto connectionDto = findOne(new Query(criteriaOne), user);
		if (connectionDto == null) {
			return null;
		}


		desensitizeMongoConnection(connectionDto);

		if (DataSourceEnum.mongodb.name().equals(connectionDto.getDatabase_type()) &&
				!DataSourceEnum.gridfs.name().equals(connectionDto.getDatabase_type())) {
			connectionDto.setDatabase_uri(UriRootConvertUtils.hidePassword(connectionDto.getDatabase_uri()));
		}

		if (StringUtils.isNotBlank(tableName)) {
			log.debug("table name is not blank");

			String qualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", connectionDto, tableName);
			Criteria criteria = Criteria.where("qualified_name").is(qualifiedName);
			Query query = new Query(criteria);
			query.fields().include("id");
			MetadataInstancesDto one = metadataInstancesService.findOne(query, user);
			if (one == null) {
				log.warn("table not found, qualifiedName = {}", qualifiedName);
				throw new BizException("Datasource.TableNotFound", "Table not found");
			}

			String databaseId = one.getId().toHexString();
			connectionDto.setConnMetadataInstanceId(databaseId);
			Criteria criteria1 = Criteria.where("databaseId").is(databaseId).and("isDeleted").is(false)
					.and("originalName").is(tableName);
			Query query1 = new Query(criteria1);
			query1.fields().include("id");
			MetadataInstancesDto one1 = metadataInstancesService.findOne(query, user);
			if (one1 == null) {
				log.warn("schema not found, databaseId = {}, originalName = {}", databaseId, tableName);
				throw new BizException("Datasource.SchemaNotFound", "Schema not found");
			}
			connectionDto.setTableMetadataInstanceId(one1.getId().toHexString());

		} else if (schema != null && schema) {
			log.debug("schema is true");
			List<String> metaTypes = new ArrayList<>();
			metaTypes.add("collection");
			metaTypes.add("table");
			metaTypes.add("file");

			//TODO 这里的第一个查询参数本来是"source._id": new RegExp(id)
			Criteria criteria = Criteria.where("source._id").is(id.toHexString()).and("metaType").in(metaTypes).and("is_deleted").is(false);
			Query query = new Query(criteria);
			//TODO 这个查询参数可能让id跟_id搞混
			query.fields().include("id", "name", "original_name");
			List<MetadataInstancesDto> tables = metadataInstancesService.findAllDto(query, user);
			if (CollectionUtils.isEmpty(tables)) {
				log.debug("tables is not found, qualified_name like {}, metaType = {}", id.toHexString(), metaTypes);
				throw new BizException("Datasource.TableNotFound", "Tables is not found");
			}

			List<Table> tableSchemas = tables.parallelStream().map(t -> {
				Table table = new Table();
				table.setTableId(t.getId().toHexString());
				table.setTableName(StringUtils.isNotBlank(t.getName()) ? t.getName() : t.getOriginalName());
				table.setCdcEnabled(true);
				return table;
			}).collect(Collectors.toList());

			Schema schemaDto = new Schema();
			schemaDto.setTables(tableSchemas);

			log.debug("schema size = {}", tableSchemas.size());
			connectionDto.setSchema(schemaDto);
		}

		return connectionDto;
	}

	public static void desensitizeMongoConnection(DataSourceConnectionDto connectionDto) {
		if (DataSourceEnum.isMongoDB(connectionDto.getDatabase_type()) && !DataSourceEnum.isGridFs(connectionDto.getDatabase_type())) {
			String databaseUri = connectionDto.getDatabase_uri();
			log.debug("parser mongo uri, result = {}", databaseUri);
			if (com.tapdata.manager.common.utils.StringUtils.isBlank(databaseUri))
				return;
			//将databaseUri解析成为标准模式的参数
			ConnectionString connectionString = null;
			try {
				connectionString = new ConnectionString(databaseUri);
			} catch (Exception e) {
				log.error("Parse connection string failed ({}) {}", databaseUri, e.getMessage());
			}

			if (connectionString != null) {
				StringBuilder hosts = new StringBuilder();
				for (String host : connectionString.getHosts()) {
					if (!host.contains(":")) {
						host = host + ":" + "27017";
					}
					hosts.append(host).append(",");
				}
				connectionDto.setDatabase_host(hosts.substring(0, hosts.length() - 1));
				connectionDto.setDatabase_username(connectionString.getUsername());
				connectionDto.setDatabase_name(connectionString.getDatabase());
			}
			if (databaseUri.contains("?")) {
				int index = databaseUri.indexOf("?");
				connectionDto.setAdditionalString(databaseUri.substring(index + 1));
			}
			//connectionDto.setDatabase_uri("");
			connectionDto.setDatabase_password(null);
		}
	}


	protected void beforeSave(DataSourceConnectionDto connection, UserDetail user) {
		log.debug("create connection before, connection = {}, user = {}", connection, user == null ? null : user.getUserId());
		//校验数据源的名称是否合法
		checkName(connection.getName());

		//根据dataSourceType查询数据源定义是否存在，如果不存在则返回数据源定义不存在的错误
		DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findByPdkHash(connection.getPdkHash(), Integer.MAX_VALUE, user);
		if (definitionDto == null) {
			throw new BizException("Data source definition not found");
		}

		connection.setDefinitionBuildNumber(String.valueOf(definitionDto.getBuildNumber()));
		connection.setDefinitionVersion(definitionDto.getVersion());
		connection.setDefinitionGroup(definitionDto.getGroup());
		connection.setDefinitionScope(definitionDto.getScope());
		connection.setDefinitionPdkId(definitionDto.getPdkId());
		connection.setDefinitionTags(definitionDto.getTags());
		connection.setCapabilities(definitionDto.getCapabilities());

		//检查是否存在名称相同的数据源连接，存在的话，则不允许。（还可以检验一下关键字）
		checkRepeatName(user, connection.getName(), connection.getId());
		//根据数据源定义信息，使用json schema校验数据源配置信息的格式，如果格式不正确则返回错误
//        boolean ok = JsonSchemaUtils.checkDataSourceDefinition(definitionDto, connection.getConfig());
//        if (!ok) {
//            throw new BizException("Data source connections config is invalid");
//        }

		//设置agentTags
		setAgentTag(connection);

		if (StringUtils.isNotBlank(connection.getPlain_password())) {
			connection.setDatabase_password(AES256Util.Aes256Encode(connection.getPlain_password()));
			connection.setPlain_password("");
		}

		connection.setTransformed(true);

		//schema 不能入库，应该是通过update接口存在metadataInstance里面
		connection.setSchema(null);

		//mongo的特殊处理
		if ((DataSourceEnum.isMongoDB(connection.getDatabase_type()) || DataSourceEnum.isGridFs(connection.getDatabase_type()))
				&& StringUtils.isBlank(connection.getDatabase_uri())) {
			log.debug("set the uri of the mongo type data source");

			connection.setDatabase_uri(UriRootConvertUtils.constructUri(connection));
			parserMongo(connection);

		} else if (StringUtils.isNotBlank(connection.getDatabase_uri())) {
			parserMongo(connection);
		}

		checkConn(connection, user);

	}

	private void setAgentTag(DataSourceConnectionDto connection) {
		log.debug("set connection agent tag, connection = {}", connection);
		List<String> tags = new ArrayList<>();
		if (connection.getPlatformInfo() != null) {
			PlatformInfo platformInfo = connection.getPlatformInfo();
			if (platformInfo.getRegion() != null) {
				tags.add(platformInfo.getRegion());
			}

			if (platformInfo.getZone() != null) {
				tags.add(platformInfo.getZone());
			}

			if (platformInfo.getAgentType() != null) {
				tags.add(platformInfo.getAgentType());
			}

			if (platformInfo.getIsThrough() != null && platformInfo.getIsThrough()) {
				tags.add("internet");
			}
		}
		log.debug("tags = {}", tags);
		connection.setAgentTags(tags);
	}


	private void parserMongo(DataSourceConnectionDto connection) {
		log.debug("parser mongo, connection = {}", connection);
		ConnectionString connectionString;
		try {
			//解析mongodb的uri 可以得到Mongo的连接信息
			connectionString = new ConnectionString(connection.getDatabase_uri());
		} catch (Exception e) {
			log.warn("Parse connection " + connection.getName() + " database uri " + connection.getDatabase_uri() + " failed " + e.getMessage());
			throw new BizException("IllegalArgument");
		}

		log.debug("parser mongo connection uri, connectionString = {}", connectionString);
		connection.setDatabase_username(connectionString.getUsername());
		if (connectionString.getPassword() != null) {
			StringBuilder psd = new StringBuilder();
			for (char c : connectionString.getPassword()) {
				psd.append(c);
			}
			connection.setDatabase_password(AES256Util.Aes256Encode(psd.toString()));
		}
	}

	/**
	 * 根据tagId删除数据源连接中的tag
	 *
	 * @param tags
	 * @param user
	 */
	public void deleteTags(List<ObjectId> tags, UserDetail user) {
		log.info("delete connection tags, tags id = {}, user = {}", tags, user == null ? null : user.getUserId());
		Update update = new Update();
		update.pull("tagList", new BasicDBObject("id", new BasicDBObject("$in", tags)));
		UpdateResult updateResult = repository.update(new Query(), update, user);

	}

	//校验数据源连接名称的正确性，这里仅仅根据正则表达式校验名称的规则，不涉及重复命名相关的检验
	private void checkName(String name) {
		log.debug("check connection name, name = {}", name);
		if (name != null) {
			boolean matches = name.matches(connectNameReg);
			if (!matches) {
				log.warn("Illegal name, name = {}", name);
				throw new BizException("Datasource.IllegalName");
			}
		}
	}


	private void deleteModels(String loadFieldsStatus, String datasourceId, Long schemaVersion, UserDetail user) {
		log.debug("delete model, loadFieldsStatus = {}, datasourceId = {}, schemaVersion = {}",
				loadFieldsStatus, datasourceId, schemaVersion);
		if ("finished".equals(loadFieldsStatus) && schemaVersion != null) {
			log.debug("loadFieldsStatus is finished, update model delete flag");
			// handle delete model, not match schemaVersion will update is_deleted to true
			Criteria criteria = Criteria.where("is_deleted").ne(true).and("source._id").is(datasourceId)
					.and("lastUpdate").ne(schemaVersion).and("taskId").exists(false).and("meta_type").ne("database");
			log.info("Delete metadata update filter: {}", criteria);
			Query query = new Query(criteria);
			Update update = Update.update("is_deleted", true);
			UpdateResult updateResult = metadataInstancesService.update(query, update, user);
		}
	}


	public void sendTestConnection(DataSourceConnectionDto connectionDto, boolean updateSchema, Boolean submit, UserDetail user) {
		log.info("send test connection, connection = {}, updateSchema = {}， submit = {}", connectionDto.getName(), updateSchema, submit);

		submit = submit != null && submit;
		if (!submit) {
			return;
		}

		if (StringUtils.isBlank(connectionDto.getPlain_password())) {
			connectionDto.setPlain_password(null);
		}

		List<Worker> availableAgent;
		if (StringUtils.isBlank(connectionDto.getAccessNodeType())
				&& StringUtils.equalsIgnoreCase(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), connectionDto.getAccessNodeType())) {
			availableAgent = workerService.findAvailableAgentByAccessNode(user, connectionDto.getAccessNodeProcessIdList());
		} else {
			availableAgent = workerService.findAvailableAgent(user);
		}
		if (CollectionUtils.isEmpty(availableAgent)) {
			Criteria.where("id").is(connectionDto.getId());
			Update updateInvalid = Update.update("status", "invalid").set("errorMsg", "no agent");
			updateByIdNotChangeLast(connectionDto.getId(), updateInvalid, user);
			log.info("send test connection, agent not found");
			return;

		}

		String processId = availableAgent.get(0).getProcessId();
		ObjectMapper objectMapper = new ObjectMapper();
		String json;
		Map<String, Object> data;
		try {
			json = objectMapper.writeValueAsString(connectionDto);
			data = objectMapper.readValue(json, Map.class);
		} catch (JsonProcessingException e) {
			log.warn("json parser failed");
			throw new BizException("SystemError");
		}
		data.put("type", "testConnection");
		data.put("editTest", false);
		data.put("updateSchema", updateSchema);
		MessageQueueDto queueDto = new MessageQueueDto();
		queueDto.setReceiver(processId);
		queueDto.setData(data);
		queueDto.setType("pipe");

		log.info("build send test connection websocket context, processId = {}, userId = {}", processId, user.getUserId());
		messageQueueService.sendMessage(queueDto);

		Update update = Update.update("status", "testing").set("testTime", System.currentTimeMillis());
		update(new Query(Criteria.where("_id").is(connectionDto.getId())), update, user);
	}

	public void checkConn(DataSourceConnectionDto connectionDto, UserDetail user) {
		log.debug("check connection duplicate, connection = {}", connectionDto);
		boolean duplicateCreate = SettingsEnum.CONNECTIONS_CREAT_DUPLICATE_SOURCE.getBoolValue(true);
		log.debug("system duplicateCreate param = {}", duplicateCreate);
		if (duplicateCreate) {
			log.debug("No check required");
			return;
		}

		Criteria criteria = new Criteria();

		if (connectionDto.getId() != null) {
			criteria.and("id").ne(connectionDto.getId());
		}

		if (DataSourceEnum.isGridFs(connectionDto.getDatabase_type()) || DataSourceEnum.isMongoDB(connectionDto.getDatabase_type())) {
			criteria.and("database_uri").is(connectionDto.getDatabase_uri());
			if (DataSourceEnum.isGridFs(connectionDto.getDatabase_type())) {
				criteria.and("file_type").is(connectionDto.getFile_type());
			}
		} else if (DataSourceDefinitionDto.PDK_TYPE.equals(connectionDto.getPdkType())) {
			//TODO 由于pdk是自定义配置，所以不太好确定这个判断怎么写。
		} else {
			criteria.and("database_type").ne(connectionDto.getDatabase_type());
			criteria.and("database_host").ne(connectionDto.getDatabase_host());
			criteria.and("database_port").ne(connectionDto.getDatabase_port());
			criteria.and("database_name").ne(connectionDto.getDatabase_name());
			criteria.and("database_username").ne(connectionDto.getDatabase_username());

			if (StringUtils.isNotBlank(connectionDto.getDatabase_owner())) {
				criteria.and("database_owner").ne(connectionDto.getDatabase_owner());
			}
		}

		DataSourceConnectionDto one = findOne(new Query(criteria), user);
		if (one != null) {
			log.info("duplicate source");
			throw new BizException("Datasource.DuplicateSource", "duplicate source");
		}
	}

	public long upsertByWhere(Where where, Document update, DataSourceConnectionDto connectionDto, UserDetail user) {


		Boolean submit = null;
		if (where != null && where.get("_id") != null) {

			String id = (String) where.get("_id");
			ObjectId objectId = toObjectId(id);
			if (connectionDto != null) {
				submit = connectionDto.getSubmit();
				connectionDto.setId(objectId);
				updateCheck(user, connectionDto);
			}

//            if (update != null) {
//                update.get("$set").pu(objectId);
//                updateCheck(user, update.getSet());
//            }

			List<TapTable> tables = null;
			boolean hasSchema = false;
			boolean rename = false;
			String oldName = null;
			Document set = null;
			Object status = null;
			if (update != null && update.get("$set") != null) {
				set = setToDocumentByJsonParser(update);
				if (set != null && (set.get("schema.tables") != null
						|| DataSourceConnectionDto.STATUS_INVALID.equals(set.get("status"))
						|| DataSourceConnectionDto.STATUS_READY.equals(set.get("status")))) {
					status = set.get("status");
					if (set.get("schema.tables") != null) {
						String tablesJson = JsonUtil.toJsonUseJackson(set.get("schema.tables"));
						tables = InstanceFactory.instance(JsonParser.class).fromJson(tablesJson, new TypeHolder<List<TapTable>>() {
						});
						set.put("schema.tables", null);
					}
					hasSchema = true;
				}
			}

			Criteria criteria = Criteria.where("_id").is(new ObjectId((String) where.get("_id")));
			//todo 删除user
			DataSourceConnectionDto oldConnectionDto = findOne(new Query(criteria), user);

			DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(oldConnectionDto.getDatabase_type(), user);

			oldConnectionDto.setDefinitionGroup(definitionDto.getGroup());
			oldConnectionDto.setDefinitionPdkId(definitionDto.getPdkId());
			oldConnectionDto.setDefinitionScope(definitionDto.getScope());
			oldConnectionDto.setDefinitionVersion(definitionDto.getVersion());

			if (connectionDto != null) {
				if (oldConnectionDto != null && StringUtils.isNotBlank(oldConnectionDto.getName()) && !oldConnectionDto.getName().equals(connectionDto.getName())) {
					rename = true;
					oldName = oldConnectionDto.getName();
				}
			}

			if (oldConnectionDto != null && oldConnectionDto.getId() != null) {
				if (connectionDto != null) {
					connectionDto.setDatabase_type(oldConnectionDto.getDatabase_type());
				}

				String connectionId = oldConnectionDto.getId().toHexString();
				oldConnectionDto.setBuildModelId(connectionId);

				MetadataInstancesDto databaseModel = MetaDataBuilderUtils.build("database", oldConnectionDto, oldConnectionDto.getUserId(), oldConnectionDto.getCreateUser());

				Criteria criteria1 = Criteria.where("qualified_name").is(databaseModel.getQualifiedName());
				MetadataInstancesDto oldMeta = metadataInstancesService.findOne(new Query(criteria1), user);

				if (set != null) {
					String tableName = (String) set.get("name");
					if (StringUtils.isNotBlank(tableName)) {
						databaseModel.setOriginalName(tableName);
						if (databaseModel.getSource() != null) {
							databaseModel.getSource().setName(tableName);
						}
					}
				}


				//model.upsertWithWhere
				if (oldMeta != null) {
					databaseModel.setHistories(oldMeta.getHistories());
					metadataUtil.addHistory(oldMeta.getId(), databaseModel, oldMeta, user, false);
				}

				databaseModel.setSourceType(SourceTypeEnum.SOURCE.name());
				databaseModel.setDeleted(false);

				databaseModel = metadataInstancesService.upsertByWhere(Where.where("qualified_name", databaseModel.getQualifiedName()), databaseModel, user);
				String databaseId = databaseModel.getId().toHexString();
				List<String> inValues = new ArrayList<>();
				inValues.add("collection");
				inValues.add("view");
				Criteria criteria2 = Criteria.where("source._id").is(connectionId).and("meta_type").in(inValues);
				metadataInstancesService.update(new Query(criteria2), Update.update("databaseId", databaseId), user);


				if (hasSchema) {
					if (CollectionUtils.isNotEmpty(tables)) {
						Long schemaVersion = (Long) set.get("lastUpdate");
						String loadFieldsStatus = (String) set.get("loadFieldsStatus");
						Boolean loadSchemaField = set.get("loadSchemaField") != null ? ((Boolean) set.get("loadSchemaField")) : true;
						loadSchema(user, tables, oldConnectionDto, definitionDto.getExpression(), databaseId, loadSchemaField);
						deleteModels(loadFieldsStatus, oldConnectionDto.getId().toHexString(), schemaVersion, user);
						update.put("loadSchemaTime", new Date());
					}
				} else {
					if (set != null && set.get("lastUpdate") != null) {
						deleteModels("finished", connectionId, (Long) set.get("lastUpdate"), user);
					}
				}
			}
			if (connectionDto != null) {
				long count = updateByWhere(where, connectionDto, user);
				if (oldConnectionDto != null) {
					updateAfter(user, oldConnectionDto, submit);
				}
				return count;
			}
			if (update != null) {
				if (update.get("$set") != null) {
					setToDocument(update);
				}

				Filter filter = new Filter(where);
				filter.setLimit(0);
				filter.setSkip(0);
				Query query = repository.filterToQuery(filter);
				Update datasourceUpdate = Update.fromDocument(update);

				if (Objects.nonNull(status)) {
					datasourceUpdate.set("testTime", System.currentTimeMillis());
					if (DataSourceEntity.STATUS_READY.equals(status.toString())) {
						datasourceUpdate.set("testCount", 0);
//						CompletableFuture.runAsync(() -> alarmService.connectAlarm(oldConnectionDto.getName(), id, datasourceUpdate.toString(), true));
					} else {
						datasourceUpdate.inc("testCount", 1);
//						CompletableFuture.runAsync(() -> alarmService.connectAlarm(oldConnectionDto.getName(), id, datasourceUpdate.toString(), false));
					}
				}
				return repository.update(query, datasourceUpdate, user).getModifiedCount();
			}
		}

		return 0L;
	}

	public void loadSchema(UserDetail user, List<TapTable> tables, DataSourceConnectionDto oldConnectionDto, String expression, String databaseId, Boolean loadSchemaField) {
		for (TapTable table : tables) {
			PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(table.getNameFieldMap() == null ? new LinkedHashMap<>() : table.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
		}

		List<MetadataInstancesDto> newModels = tables.stream().map(tapTable -> {
			MetadataInstancesDto instance = PdkSchemaConvert.fromPdk(tapTable);
			instance.setAncestorsName(instance.getOriginalName());
			return instance;
		}).collect(Collectors.toList());
		//List<MetadataInstancesDto> newModels = SchemaTransformUtils.oldSchema2newSchema(schema);
		log.info("upsert new models into MetadataInstance: {}, connection id = {}, connection name = {}",
				newModels.size(), oldConnectionDto.getId().toHexString(), oldConnectionDto.getName());

		if (CollectionUtils.isNotEmpty(newModels)) {
			for (MetadataInstancesDto newModel : newModels) {
				List<Field> fields = newModel.getFields();
				if (CollectionUtils.isNotEmpty(fields)) {
					for (Field field : fields) {
						field.setSourceDbType(oldConnectionDto.getDatabase_type());
						String originalDataType = field.getDataType();
						if (StringUtils.isEmpty(field.getOriginalDataType())) {
							field.setOriginalDataType(originalDataType);
						}
					}
				}
			}

			oldConnectionDto.setLoadSchemaField(loadSchemaField);
			List<MetadataInstancesDto> newModelList = metadataUtil.modelNext(newModels, oldConnectionDto, databaseId, user);

			Pair<Integer, Integer> pair = metadataInstancesService.bulkUpsetByWhere(newModelList, user);
			List<String> qualifiedNames = newModelList.stream().filter(Objects::nonNull).map(MetadataInstancesDto::getQualifiedName)
					.filter(StringUtils::isNotBlank).collect(Collectors.toList());
			metadataInstancesService.qualifiedNameLinkLogic(qualifiedNames, user);
			String name = newModelList.stream().map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList()).toString();
			log.info("Upsert model, model list = {}, values = {}, modify count = {}, insert count = {}"
					, newModelList.size(), name, pair.getLeft(), pair.getRight());
		}
	}

	private Document setToDocumentByJsonParser(Document update) {
		Document set;
		String setJson = JsonUtil.toJsonUseJackson(update.get("$set"));
		set = InstanceFactory.instance(JsonParser.class).fromJson(setJson, Document.class);
		update.put("$set", set);
		return set;
	}

	private Document setToDocument(Document update) {
		Document set;
		String setJson = JsonUtil.toJsonUseJackson(update.get("$set"));
		set = JsonUtil.parseJsonUseJackson(setJson, new TypeReference<Document>() {
		});
		update.put("$set", set);
		return set;
	}


	public void updateAfter(UserDetail user, DataSourceConnectionDto connectionDto, String oldName, Boolean submit) {
//        UserLogs userLogs = new UserLogs();
//        userLogs.setUserId(user.getUserId());
//        userLogs.setModular(MODULAR);
//        userLogs.setOperation("update");
//        userLogs.setParameter1(connectionDto.getName());
//        userLogs.setParameter2("");
//        userLogs.setParameter3("");
//        userLogs.setSourceId(connectionDto.getId());
//
//        if (StringUtils.isNotBlank(oldName)) {
//            userLogs.setRename(true);
//            userLogs.setParameter2(oldName);
//        }
//        //userLogService.add(userLogs);

		if (connectionDto.getId() != null && connectionDto.getName() != null) {
			Criteria criteria = Criteria.where("source.id").is(connectionDto.getId()).and("meta_type").is("database");
			Update update = Update.update("original_name", connectionDto.getName()).set("source.name", connectionDto.getName());
			metadataInstancesService.update(new Query(criteria), update, user);
		}

		//更新数据目录
		if (StringUtils.isNotBlank(oldName)) {
			defaultDataDirectoryService.updateConnection(connectionDto, user);
		}

		sendTestConnection(connectionDto, true, submit, user);
		desensitizeMongoConnection(connectionDto);
	}

	public void updateAfter(UserDetail user, DataSourceConnectionDto connectionDto, Boolean submit) {
		String name = null;
		String sourceId = null;
		if (connectionDto != null) {
			name = connectionDto.getName();
			sourceId = connectionDto.getId().toHexString();

			sendTestConnection(connectionDto, false, submit, user);
		}
		//userLogService.addUserLog(Modular.CONNECTION, name, OperationType.UPDATE, user, sourceId);
		desensitizeMongoConnection(connectionDto);
	}

	public List<String> distinct(String field, UserDetail user) {
		List<String> distinct = repository.findDistinct(new Query(), field, user, String.class);
		return distinct;
	}

	/**
	 * 获取所有支持的数据库类型
	 *
	 * @param user
	 * @return
	 */
	public List<String> databaseType(UserDetail user) {
		Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.FRONTEND, KeyEnum.ALLOW_CONNECTION_TYPE);
		String allowDatabaseType = (String) settings.getValue();
		List<String> supportDbTypeList = Arrays.asList(allowDatabaseType.split(","));
		List<String> allDbType = distinct("database_type", user);
		allDbType.retainAll(supportDbTypeList);
		return allDbType;
	}

	/**
	 * 校验表是否属于某个数据库
	 * 查询条件   {is_deleted:false,"source._id":"61e51d6cea6ade24c6bae75c",meta_type:{$in:["table", "collection", "view"]}}
	 *
	 * @param connectionId
	 * @return
	 */
	public ValidateTableVo validateTable(String connectionId, List<String> tableList) {
		ValidateTableVo validateTableVo = new ValidateTableVo();
		//通过管理metaInstance source.id来判断表是否存在
		Criteria criteria = Criteria
				.where("meta_type").in("table", "collection", "view")
//                .and("original_name").in(tableList)
				.and("source._id").is(connectionId)
				.and("is_deleted").is(false);
		Query query = Query.query(criteria);
		List<MetadataInstancesDto> metadataInstances = metadataInstancesService.findAll(query);
		if (CollectionUtils.isNotEmpty(metadataInstances)) {
			List<String> originalNameList = metadataInstances.stream().filter(metadataInstancesDto -> (StringUtils.isNotEmpty(metadataInstancesDto.getOriginalName()))).map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList());
			tableList.removeAll(originalNameList);
		}
		validateTableVo.setTableNotExist(tableList);
		return validateTableVo;
	}


	public DataSourceConnectionDto findOne(Filter filter, UserDetail user, Boolean noSchema) {
		Query query = repository.filterToQuery(filter);
		DataSourceConnectionDto connectionDto = findOne(query, user);
		if (connectionDto == null) {
			return null;
		}

		if (noSchema != null) {
			List<DataSourceConnectionDto> items = new ArrayList<>();
			items.add(connectionDto);
			Map<ObjectId, DataSourceConnectionDto> map = buildFindResult(noSchema, items, user);
			return map.get(connectionDto.getId());
		} else {
			return connectionDto;
		}
	}

	public List<DataSourceConnectionDto> findInfoByConnectionIdList(List<String> connectionIdList) {
		if (CollectionUtils.isEmpty(connectionIdList)) {
			return Lists.newArrayList();
		}
		Criteria criteria = Criteria.where("_id").in(connectionIdList);
		Query query = new Query(criteria);
		return findAll(query);
	}

	public List<DataSourceConnectionDto> findInfoByConnectionIdList(List<String> connectionIdList, UserDetail user, String... fields) {
		if (CollectionUtils.isEmpty(connectionIdList)) {
			return Lists.newArrayList();
		}
		Criteria criteria = Criteria.where("_id").in(connectionIdList);
		Query query = new Query(criteria);
		query.fields().include(fields);
		return findAllDto(query, user);
	}

	public Map<String, DataSourceConnectionDto> batchImport(List<DataSourceConnectionDto> connectionDtos, UserDetail user, boolean cover) {

		Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
		for (DataSourceConnectionDto connectionDto : connectionDtos) {
			String connId = connectionDto.getId().toHexString();
			Query query = new Query(Criteria.where("_id").is(connectionDto.getId()));
			query.fields().include("_id");
			DataSourceConnectionDto connection = findOne(query);
			if (connection == null) {
				while (checkRepeatNameBool(user, connectionDto.getName(), null)) {
					connectionDto.setName(connectionDto.getName() + "_import");
				}
				connection = importEntity(connectionDto, user);
			} else {
				if (cover) {
					ObjectId objectId = connection.getId();
					while (checkRepeatNameBool(user, connectionDto.getName(), objectId)) {
						connectionDto.setName(connectionDto.getName() + "_import");
					}

					connectionDto.setListtags(null);
					connectionDto.setAccessNodeProcessId(null);
					connectionDto.setAccessNodeProcessIdList(new ArrayList<>());
					connectionDto.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());


					connection = save(connectionDto, user);
				}
			}

			conMap.put(connId, connection);

		}
		return conMap;
	}

	public List<DataSourceConnectionDto> listAll(Filter filter, UserDetail loginUser) {
		Query query = repository.filterToQuery(filter);
		query.skip(0);
		query.limit(0);
		return findAllDto(query, loginUser);
	}

	public List<String> findIdByName(String name) {
		List<String> connectionIds = Collections.emptyList();
		Query query = Query.query(Criteria.where("name").regex(name));
		List<DataSourceConnectionDto> dataSourceConnectionDtoList = findAll(query);
		if (CollectionUtils.isNotEmpty(dataSourceConnectionDtoList)) {
			List<ObjectId> connectionObjectIds = dataSourceConnectionDtoList.stream().map(DataSourceConnectionDto::getId).collect(Collectors.toList());
			connectionIds = connectionObjectIds.stream().map(ObjectId::toString).collect(Collectors.toList());
		}
		return connectionIds;
	}

	/**
	 * 现在除了 kafka, elasticsearch以外  都支持数据校验
	 *
	 * @param userDetail
	 * @return 数据结构方面最好能够兼容数据源的版本信息
	 * 比如这样：
	 * [
	 * {
	 * name: 'mysql',
	 * supportList: [
	 * {
	 * version: '8.0',
	 * isSupportValification: true,
	 * canBeTarget: true,
	 * canBeSource: true
	 * <p>
	 * },
	 * {
	 * version: '9.0'
	 * }
	 * ]
	 * }
	 * ]
	 */
	public List<SupportListVo> supportList(UserDetail userDetail) {
		List<SupportListVo> supportListVoList = new ArrayList<>();
		List<LibSupportedsEntity> libSupportedsEntityList = libSupportedsRepository.getMongoOperations().findAll(LibSupportedsEntity.class);
		libSupportedsEntityList.forEach(libSupportedsEntity -> {
			SupportListVo supportListVo = new SupportListVo(new ArrayList<>());
			supportListVo.setName(libSupportedsEntity.getDatabaseType());

			List<Map<String, Object>> setSupportList = new ArrayList<>();
			Map<String, Object> supportListMap = new HashMap<>();

			supportListMap = (Map<String, Object>) libSupportedsEntity.getSupportedList();
			supportListMap.put("version", "");

			Object supportInspect = supportListMap.remove("supportInspect");
			if (null == supportInspect) {
				supportInspect = false;
			}
			supportListMap.put("supportInspect", supportInspect);
			setSupportList.add(supportListMap);
			supportListVo.setSupportList(setSupportList);
			supportListVoList.add(supportListVo);
		});
		return supportListVoList;

	}

	public List<DataSourceConnectionDto> findAllByIds(List<String> ids) {
		if (CollectionUtils.isEmpty(ids)) {
			return Lists.newArrayList();
		}

		List<ObjectId> objectIds = ids.stream().map(ObjectId::new).collect(Collectors.toList());

		Criteria criteria = Criteria.where("_id").in(objectIds);
		Query query = new Query(criteria);
		return findAll(query);
	}


	public void updateConnectionOptions(ObjectId id, ConnectionOptions options, UserDetail user) {

		if (options == null) {
			options = new ConnectionOptions();
		}

		Criteria criteria = Criteria.where("_id").is(id);
		Query query = new Query(criteria);
		query.fields().include("_id", "database_type");
		DataSourceConnectionDto connectionDto = findOne(query, user);
		if (connectionDto == null) {
			return;
		}

		Update update = new Update();

		if (options.getCapabilities() == null) {
			options.setCapabilities(Lists.newArrayList());
		}

		DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(connectionDto.getDatabase_type(), user);
		List<Capability> capabilities = definitionDto.getCapabilities();
		List<Capability> updateCapabilities = options.getCapabilities();
		if (CollectionUtils.isNotEmpty(updateCapabilities)) {
			Set<String> upCapabilitySet = updateCapabilities.stream().map(Capability::getId).collect(Collectors.toSet());
			for (Capability capability : capabilities) {
				if (!upCapabilitySet.contains(capability.getId())) {
					updateCapabilities.add(capability);
				}
			}
		} else {
			updateCapabilities.addAll(capabilities);
		}
		update.set("capabilities", updateCapabilities);


		if (StringUtils.isNotBlank(options.getConnectionString())) {
			update.set("connectionString", options.getConnectionString());
		}

		if (options.getTimeZone() != null) {
			update.set("timeZone", options.getTimeZone());
		}

		updateByIdNotChangeLast(id, update, user);

	}

	public Long countTaskByConnectionId(String connectionId, UserDetail userDetail) {
		Query query = new Query(Criteria.where("dag.nodes.connectionId").is(connectionId)
				.and("syncType").ne(TaskDto.SYNC_TYPE_CONN_HEARTBEAT)
				.andOperator(Criteria.where("is_deleted").is(false),Criteria.where("status").ne("delete_failed")));
		query.fields().include("_id", "name", "syncType");
		return taskService.count(query, userDetail);
	}
	public List<TaskDto> findTaskByConnectionId(String connectionId, int limit, UserDetail userDetail) {
		Query query = new Query(Criteria.where("dag.nodes.connectionId").is(connectionId)
				.and("syncType").ne(TaskDto.SYNC_TYPE_CONN_HEARTBEAT)
				.andOperator(Criteria.where("is_deleted").is(false),Criteria.where("status").ne("delete_failed")));
		query.fields().include("_id", "name", "syncType");
		query.limit(limit);
		query.with(Sort.by(Sort.Direction.ASC, "_id"));
		return taskService.findAllDto(query, userDetail);
	}

	public ConnectionStats stats(UserDetail userDetail) {

		Aggregation aggregation = Aggregation.newAggregation(
				match(Criteria.where("user_id").is(userDetail.getUserId()).and("customId").is(userDetail.getCustomerId())),
				group("status").count().as("count")
		);

		AggregationResults<Part> result = repository.aggregate(aggregation, Part.class);

		Iterator<Part> it = result.iterator();
		Map<String, Long> data = new HashMap<>();
		while(it.hasNext()) {
			Part part = it.next();
			data.put(part.get_id(), part.getCount());
		}

		ConnectionStats connectionStats = new ConnectionStats();
		if (data.containsKey(DataSourceConnectionDto.STATUS_INVALID)) {
			connectionStats.setInvalid(data.get(DataSourceConnectionDto.STATUS_INVALID));
		}
		if (data.containsKey(DataSourceConnectionDto.STATUS_READY)) {
			connectionStats.setReady(data.get(DataSourceConnectionDto.STATUS_READY));
		}
		if (data.containsKey(DataSourceConnectionDto.STATUS_TESTING)) {
			connectionStats.setTesting(data.get(DataSourceConnectionDto.STATUS_TESTING));
		}
		connectionStats.setTotal(connectionStats.getInvalid() + connectionStats.getReady() + connectionStats.getTesting());

		return connectionStats;
	}

	public void loadPartTables(String connectionId, List<TapTable> tables, UserDetail user) {
		DataSourceConnectionDto connectionDto = findById(toObjectId(connectionId));
		if (connectionDto == null) {
			return;
		}

		DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(connectionDto.getDatabase_type(), user);
		if (definitionDto == null) {
			return;
		}

		Criteria criteria = Criteria.where("source._id").is(connectionId).and("meta_type").is("database");
		Query query = new Query(criteria);
		query.fields().include("_id");

		MetadataInstancesDto databaseModel = metadataInstancesService.findOne(query, user);
		String databaseModelId = databaseModel.getId().toHexString();
		if (StringUtils.isBlank(databaseModelId)) {
			return;
		}

		loadSchema(user, tables, connectionDto, definitionDto.getExpression(), databaseModelId, true);
	}

	public void batchEncryptConfig() {
		Query query = Query.query(Criteria
				.where("config").ne(null)
				.and("encryptConfig").exists(false));
		query.fields().include("_id", "config");
		List<DataSourceEntity> result = repository.findAll(query);
		result.forEach(entity -> {

			repository.encryptConfig(entity);

			if (entity.getEncryptConfig() != null) {
				repository.update(Query.query(Criteria.where("id").is(entity.getId())),
						Update.update("encryptConfig", entity.getEncryptConfig()).unset("config"));
			}
		});
	}

    @Data
	protected static class Part{
		private String _id;
		private long count;
	}


	public DataSourceConnectionDto importEntity(DataSourceConnectionDto dto, UserDetail userDetail) {
		DataSourceEntity dataSourceEntity = repository.importEntity(convertToEntity(DataSourceEntity.class, dto), userDetail);
		return convertToDto(dataSourceEntity, DataSourceConnectionDto.class);
	}


	public DataSourceConnectionDto addConnection(DataSourceConnectionDto connectionDto, UserDetail userDetail) {

		DataSourceDefinitionDto dataSourceDefinitionDto =
				dataSourceDefinitionService.getMongoDbByDataSourceType(connectionDto.getDatabase_type());
		connectionDto.setPdkType("pdk");
		connectionDto.setPdkHash(dataSourceDefinitionDto.getPdkHash());
		return add(connectionDto,userDetail);
	}

}
