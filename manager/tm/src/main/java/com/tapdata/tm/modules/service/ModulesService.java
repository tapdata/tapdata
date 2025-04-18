package com.tapdata.tm.modules.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.cglib.CglibUtil;
import com.deepoove.poi.XWPFTemplate;
import com.deepoove.poi.config.Configure;
import com.deepoove.poi.plugin.highlight.HighlightRenderPolicy;
import com.deepoove.poi.plugin.table.LoopRowTableRenderPolicy;
import com.deepoove.poi.plugin.toc.TOCRenderPolicy;
import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.entity.ApiCallStatsEntity;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.constant.ApiTypeEnum;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.constant.ParamTypeEnum;
import com.tapdata.tm.modules.dto.*;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.param.ApiDetailParam;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.modules.vo.*;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.utils.*;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tapdata.tm.utils.DocumentUtils.getLong;

/**
 * @Author:
 * @Date: 2021/10/14
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ModulesService extends BaseService<ModulesDto, ModulesEntity, ObjectId, ModulesRepository> {
	private ApplicationService applicationService;
	private DataSourceService dataSourceService;
	private MetadataInstancesService metadataInstancesService;
	private FileService fileService;
	private ApiCallService apiCallService;
	private DataSourceDefinitionService dataSourceDefinitionService;
	private ApiCallStatsService apiCallStatsService;
	private ApiCallMinuteStatsService apiCallMinuteStatsService;

	public ModulesService(@NonNull ModulesRepository repository) {
		super(repository, ModulesDto.class, ModulesEntity.class);
	}

	private final Long oneMinuteMillSeconds = 1000L * 60;

	@Override
	protected void beforeSave(ModulesDto modules, UserDetail user) {
		if (CollectionUtils.isNotEmpty(modules.getPaths())) {
			modules.getPaths().forEach(path -> {
				if (CollectionUtils.isNotEmpty(path.getFields())) {
					path.setFields(path.getFields().stream().filter(Objects::nonNull).collect(Collectors.toList()));
				}
			});
		}
	}


	public ModulesDetailVo findById(String id) {
		ModulesDto modulesDto = findById(MongoUtils.toObjectId(id));
		ModulesDetailVo modulesDetailVo = BeanUtil.copyProperties(modulesDto, ModulesDetailVo.class);

		String connectionId = modulesDto.getConnection().toString();
		DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
		if (null != dataSourceConnectionDto) {
			dataSourceConnectionDto.setDatabase_password(null);
			dataSourceConnectionDto.setPlain_password(null);
			modulesDetailVo.setSource(dataSourceConnectionDto);
		}
		modulesDetailVo.setConnection(connectionId);
		return modulesDetailVo;
	}

	public Page findModules(Filter filter, UserDetail userDetail) {
		//不需要设定fields
		filter.setFields(null);
		Map where = filter.getWhere();
		Map notDeleteMap = new HashMap();
		notDeleteMap.put("$ne", true);
		where.put("is_deleted", notDeleteMap);


		String status = (String) where.getOrDefault("status", "");
		if ("all".equals(status)) {
			where.remove("status");
		}

		Page page = find(filter, userDetail);

		String createUser = "";
		List<ModulesListVo> modulesListVoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(page.getItems(), ModulesListVo.class);
		if (CollectionUtils.isNotEmpty(modulesListVoList)) {
			for (ModulesListVo modulesListVo : modulesListVoList) {
				String connectionId = modulesListVo.getConnection();
				if (null != connectionId) {
					DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
					if (null != dataSourceConnectionDto) {
						Source source = new Source(dataSourceConnectionDto.getDatabase_type(), dataSourceConnectionDto.getId().toString(), dataSourceConnectionDto.getName(), dataSourceConnectionDto.getStatus());
						modulesListVo.setSource(source);
					}
				}
				createUser = modulesListVo.getCreateUser() == null ? userDetail.getEmail() : modulesListVo.getCreateUser();
				modulesListVo.setUser(createUser);
			}
		}
		page.setItems(modulesListVoList);
		return page;
	}


	/**
	 * base+version 不能重复
	 *
	 * @param modulesDto
	 * @param userDetail
	 * @return
	 */
	@Override
	public ModulesDto save(ModulesDto modulesDto, UserDetail userDetail) {
//        if (findByName(modulesDto.getName()).size() > 0) {
//            throw new BizException("Modules.Name.Existed");
//        }
//        if (!isBasePathAndVersionRepeat(modulesDto.getBasePath(), modulesDto.getApiVersion()).isEmpty()) {
//            throw new BizException("Modules.BasePathAndVersion.Existed");
//        }
//        if (null == modulesDto.getDataSource()) {
//            throw new BizException("Modules.Connection.Null");
//        }
		if (findByName(modulesDto.getName()).size() > 1)
			throw new BizException("Modules.Name.Existed");
		modulesDto.setConnection(MongoUtils.toObjectId(modulesDto.getDataSource()));
		modulesDto.setLastUpdAt(new Date());
		modulesDto.setCreateAt(new Date());
		//user表  admin@admin.com  的username 没有这个字段?
		modulesDto.setCreateUser(userDetail.getUsername());
		modulesDto.setLastUpdBy(userDetail.getUsername());
		if (StringUtils.isBlank(modulesDto.getStatus())) {
			modulesDto.setStatus(ModuleStatusEnum.GENERATING.getValue());
		}
		return super.save(modulesDto, userDetail);

	}


	public ModulesDto updateModuleById(ModulesDto modulesDto, UserDetail userDetail) {
		Where where = new Where();
		where.put("id", modulesDto.getId().toString());

		Query query = new Query();
		ObjectId id = modulesDto.getId();
		if (id != null) {
			Criteria criteria = Criteria.where("_id").is(id);
			query.addCriteria(criteria);
		}
		//没有生成的接口 不能发布
		ModulesDto dto = findOne(query, userDetail);
		if (dto == null)
			throw new BizException("current module not exist");
		if (ModuleStatusEnum.ACTIVE.getValue().equals(modulesDto.getStatus()) && ModuleStatusEnum.GENERATING.getValue().equals(dto.getStatus()))
			throw new BizException("generating status can't release");
		//点击生成按钮 才校验(撤销发布等不校验)
		if (ModuleStatusEnum.PENDING.getValue().equals(modulesDto.getStatus()) && !ModuleStatusEnum.ACTIVE.getValue().equals(dto.getStatus())) {
			if (findByName(modulesDto.getName()).size() > 1)
				throw new BizException("Modules.Name.Existed");
			if (isBasePathAndVersionRepeat(modulesDto.getBasePath(), modulesDto.getApiVersion()).size() > 1)
				throw new BizException("Modules.BasePathAndVersion.Existed");
			checkoutInputParamIsValid(modulesDto);
		}
		return super.upsertByWhere(where, modulesDto, userDetail);
	}

	public List<ModulesDto> batchUpdateModuleByList(List<ModulesDto> modulesDtos, UserDetail userDetail) {
		List<ModulesDto> modulesDtoList = new ArrayList<>();
		modulesDtos.forEach((modulesDto -> modulesDtoList.add(updateModuleById(modulesDto, userDetail))));
		return modulesDtoList;
	}


	/**
	 * 复制api
	 *
	 * @param id
	 * @param userDetail
	 * @return
	 */
	public ModulesDto copy(String id, UserDetail userDetail) {
		ModulesDto existedModulesDto = findById(MongoUtils.toObjectId(id));
		if (null == existedModulesDto) {
			throw new BizException("Modules.Not.Existed");
		}
		String copyName = existedModulesDto.getName() + "_Copy";
		String copyBasePath = existedModulesDto.getBasePath() + "_Copy";
		existedModulesDto.setId(null);
		existedModulesDto.setBasePath(copyBasePath);

		existedModulesDto.setName(copyName);
		existedModulesDto.setStatus(ModuleStatusEnum.PENDING.getValue());
		save(existedModulesDto, userDetail);
		return existedModulesDto;
	}


	public Map getSchema(String id, UserDetail userDetail) {
		ModulesDto modulesDto = findById(MongoUtils.toObjectId(id), userDetail);
		if (null == modulesDto) {
			throw new BizException("Modules.Not.Existed");
		}
		Map retMap = new HashMap();
		retMap.put("fields", modulesDto.getFields());
		return retMap;
	}


	public List<ModulesDto> findByName(String name) {
		Query query = Query.query(Criteria.where("name").is(name).and("is_deleted").ne(true));
		List<ModulesDto> modulesDtoList = findAll(query);
		return modulesDtoList;
	}

	/**
	 * * upsert  0跳过已有数据  1 覆盖已有数据
	 *
	 * @param json
	 * @param upsert
	 * @param userDetail
	 * @param listTag
	 */
	public void importData(String json, String upsert, List<Tag> listTag, UserDetail userDetail) {
		Map map = JsonUtil.parseJson(json, Map.class);
		List<Map<String, Object>> data = (List) map.get("data");

		if (CollectionUtils.isNotEmpty(data)) {
			for (Map<String, Object> singleDataMap : data) {
				Query query = new Query();
				String id = singleDataMap.get("id").toString();
				singleDataMap.remove("id");
				if (StringUtils.isNotBlank(id)) {
					ModulesDto existedDto = findById(MongoUtils.toObjectId(id));
					Map existedPoperty = BeanUtil.beanToMap(existedDto);
					if ("0".equals(upsert)) {
//                        跳过已有数据,
						singleDataMap.forEach((key, value) ->
						{
							if (existedPoperty.containsKey(key)) {
								singleDataMap.put(key, existedPoperty.get(key));
							}
						});

					}
					query.addCriteria(Criteria.where("id").is(id));
				}
				ModulesDto newDto = new ModulesDto();
				for (String key : singleDataMap.keySet()) {
					if ("connection".equals(key)) {
						BeanUtil.setFieldValue(newDto, key, MongoUtils.toObjectId((String) singleDataMap.get(key)));
					} else if ("user_id".equals(key)) {
						BeanUtil.setFieldValue(newDto, key, MongoUtils.toObjectId((String) singleDataMap.get(key)));
					} else {
						BeanUtil.setFieldValue(newDto, key, singleDataMap.get(key));
					}

				}

				if (CollectionUtils.isNotEmpty(listTag)) {
					newDto.setListtags(listTag);
				}
				newDto.setIsDeleted(false);
				super.upsert(query, newDto, userDetail);
			}
		}
	}


	/**
	 * 查找已经发布的api
	 *
	 * @param userDetail
	 * @return
	 */
	public ApiDefinitionVo apiDefinition(UserDetail userDetail) {
		List<ConnectionVo> connectionVos = new ArrayList<>();
		ApiDefinitionVo apiDefinitionVo = new ApiDefinitionVo();
		//查找已发布的api
		List<ModulesDto> apis = findAllActiveApi(ModuleStatusEnum.ACTIVE);
		if (CollectionUtils.isNotEmpty(apis)) {
			List<ObjectId> connections = apis.stream().map(ModulesDto::getConnection).collect(Collectors.toList());
			Query query = Query.query(Criteria.where("id").in(connections));
			List<DataSourceConnectionDto> dataSourceConnectionDtoList = dataSourceService.findAll(query);

			//设置上pdk的APIserverkey属性
			for (DataSourceConnectionDto dataSourceConnectionDto : dataSourceConnectionDtoList) {

				Map<String, Object> config = dataSourceConnectionDto.getConfig();
				if (dataSourceConnectionDto.getDatabase_type().toLowerCase(Locale.ROOT).contains("mongo")) {
					if (config.get("uri") == null) {
						StringBuilder sb = new StringBuilder("mongodb://");
						if (config.get("user") != null) {
							String user = (String) config.get("user");
							String password = (String) config.get("password");
							try {
								user = URLEncoder.encode(user, "UTF-8");
								password = URLEncoder.encode(password, "UTF-8");
							} catch (Exception e) {
								throw new BizException("SystemError");
							}
							sb.append(user).append(":")
									.append(password).append("@");
						}
						sb.append(config.get("host")).append("/").append(config.get("database"));
						if (config.get("additionalString") != null) {
							sb.append("?").append(config.get("additionalString"));
						}

						config.put("uri", sb.toString());
					} else {
						String uri = (String) config.get("uri");
						ConnectionString connectionString = new ConnectionString(uri);
						String username = connectionString.getUsername();
						char[] passwordChar = connectionString.getPassword();
						String password = null;
						if (passwordChar != null) {
							password = new String(passwordChar);
						}

						try {
							if (StringUtils.isNotBlank(username)) {
								String newUsername = URLEncoder.encode(username, "UTF-8");
								uri = uri.replace(username, newUsername);
							}

							if (StringUtils.isNotBlank(password)) {
								String newPassword = URLEncoder.encode(password, "UTF-8");
								uri = uri.replace(password, newPassword);
							}
						} catch (Exception e) {
							throw new BizException("SystemError");
						}
						config.put("uri", uri);

					}
				}
				DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(dataSourceConnectionDto.getDatabase_type(), userDetail);
				Map<String, Object> properties = definitionDto.getProperties();
				LinkedHashMap connection = (LinkedHashMap) properties.get("connection");
				analyzeApiServerKey(dataSourceConnectionDto, connection, null);

				ConnectionVo connectionVo = cn.hutool.core.bean.BeanUtil.copyProperties(dataSourceConnectionDto, ConnectionVo.class);
				if (null != connectionVo) {
					String plainPassword = AES256Util.Aes256Decode(connectionVo.getDatabase_password());
					connectionVo.setDatabase_password(plainPassword);

					switch (dataSourceConnectionDto.getDatabase_type().toLowerCase(Locale.ROOT)) {
						case "oracle":
							if ("SID".equals(config.get("thinType"))) {
								Optional.ofNullable(config.get("sid")).map(o -> {
									connectionVo.setDatabase_name(o.toString());
									return o;
								});
							}
							break;
						default:
							break;
					}
				}
				connectionVos.add(connectionVo);
			}
			apiDefinitionVo.setConnections(connectionVos);
			apiDefinitionVo.setApis(apis);
		}
		return apiDefinitionVo;
	}

	private void analyzeApiServerKey(DataSourceConnectionDto dataSourceConnectionDto, LinkedHashMap connection, String parent) {
		LinkedList<LinkedHashMap> linkedList = new LinkedList<>();
		linkedList.offer((LinkedHashMap) connection.get("properties"));
		while (!linkedList.isEmpty()) {
			LinkedHashMap<String, Object> properties1 = linkedList.poll();
			if (null == properties1) {
				properties1 = new LinkedHashMap();
			}
			properties1.forEach((k, v) -> {
				LinkedHashMap<String, Object> v1 = (LinkedHashMap) v;
				String key = StringUtils.isBlank(parent) ? (String) k : parent + "." + k;
				if ("object".equals(((LinkedHashMap<?, ?>) v).get("type"))) {
					analyzeApiServerKey(dataSourceConnectionDto, v1, key);
				}
				String apiServerKey = (String) v1.get("apiServerKey");
				if (StringUtils.isNotBlank(apiServerKey)) {
					setApiServerKey(dataSourceConnectionDto, key, apiServerKey);
				}
				LinkedHashMap<String, Object> properties = (LinkedHashMap) v1.get("properties");
				if (null != properties) {
					linkedList.offer(properties);
				}
			});
		}
	}

	private void setApiServerKey(DataSourceConnectionDto dataSourceConnectionDto, String k, String apiServerKey) {
		Map<String, Object> config = dataSourceConnectionDto.getConfig();
		Object value = getValue(k, config);
		Class<? extends DataSourceConnectionDto> aClass = dataSourceConnectionDto.getClass();
		for (java.lang.reflect.Field declaredField : aClass.getDeclaredFields()) {
			declaredField.setAccessible(true);
			if (declaredField.getName().equals(apiServerKey)) {
				try {
					if (value == null) {
						continue;
					}
					if (declaredField.getType().equals(value.getClass())) {
						declaredField.set(dataSourceConnectionDto, value);
					} else {
						if (declaredField.getType().getName().contains("Integer") || value.getClass().getName().equals("String")) {
							declaredField.set(dataSourceConnectionDto, Integer.parseInt((String) value));
						} else if (declaredField.getType().getName().contains("String")) {
							declaredField.set(dataSourceConnectionDto, String.valueOf(value));
						}
					}
				} catch (IllegalAccessException e) {
					throw new BizException("System error");
				}
			}
		}
	}

	private Object getValue(String k, Map<String, Object> config) {
		if (k.contains(".")) {
			int i = k.indexOf(".");
			String key1 = k.substring(0, i);
			Map config1 = (Map) config.get(key1);
			String key2 = k.substring(i + 1);
			return getValue(key2, config1);

		} else {
			return config.get(k);
		}
	}

	public List<ModulesDto> getByCollectionName(String connection_id, String collectionName, UserDetail user) {
		if (connection_id != null) {
			Criteria criteria1 = Criteria.where("datasource").is(connection_id).and("tablename")
					.is(collectionName).and("status").is("active");
			List<ModulesDto> allDto = findAllDto(Query.query(criteria1), user);
			return allDto;
		}
		Criteria criteria = Criteria.where("tablename").is(collectionName).and("status").is("active");
		return findAllDto(Query.query(criteria), user);
	}

	//todo  公共库
     /* public ModulesDto apiPermission(UserDetail loginUser,ModulesDto modulesDto){
      if (modulesDto.getAccess_token()!=null){

      }
      }*/
	public Map<String, Object> getApiDocument(ObjectId id, UserDetail userDetail) {
		Query query = new Query();
		if (id != null) {
			Criteria criteria = Criteria.where("_id").is(id);
			query.addCriteria(criteria);
		}
		ModulesDto allDto = findOne(query, userDetail);
		Map<String, Object> map = new HashMap<>();
		map.put("name", allDto.getName());//getName
		map.put("status", allDto.getStatus());//getStatus
		map.put("format", "JSON");
		map.put("user", allDto.getUser());//getUser
		map.put("createUser", userDetail.getEmail());
		map.put("listtags", allDto.getListtags());//getListTags
		map.put("last_updated", allDto.getLast_updated());//getLast_updated
		map.put("apiVersion", allDto.getApiVersion());//getApiVersion
		map.put("basePath", allDto.getBasePath());
		map.put("paths", new ArrayList<>());//getBasePath
		//getPaths
		List<Field> fields = getFileds(allDto.getFields(), null, null);//getFields
		for (int i = 0; i < allDto.getPaths().size(); i++) {//getPaths
			Path pathObj = allDto.getPaths().get(i);//getPaths
			if ("defaultApi".equals(allDto.getApiType())) {//getApiType
				if ("create".equals(pathObj.getName())) {
					List<Map<String, Object>> paths = createApiDoc(pathObj, fields);
					for (Map<String, Object> path : paths) {
						map.put("paths", path);
					}
				} else if (Objects.equals(pathObj.getName(), "findById")) {
					List<Map<String, Object>> paths = findByIdDoc(pathObj, fields);
					for (Map<String, Object> path : paths) {
						map.put("paths", path);
					}
				} else if (Objects.equals(pathObj.getName(), "updateById")) {
					List<Map<String, Object>> paths = updateByIdDoc(pathObj, fields);
					for (Map<String, Object> path : paths) {
						map.put("paths", path);
					}
				} else if (Objects.equals(pathObj.getName(), "deleteById")) {
					List<Map<String, Object>> paths = deleteByIdDoc(pathObj, fields);
					for (Map<String, Object> path : paths) {
						map.put("paths", path);
					}
				} else if (Objects.equals(pathObj.getName(), "findPage")) {
					List<Map<String, Object>> paths = findByIdDoc(pathObj, fields);
					for (Map<String, Object> path : paths) {
						map.put("paths", path);
					}
				}
			} else if (Objects.equals(pathObj.getName(), "GET")) {
				List<Map<String, Object>> paths = custGetDoc(pathObj, fields);
				for (Map<String, Object> path : paths) {
					map.put("paths", path);
				}
			}
		}
		return map;

	}

	public List<Map<String, Object>> findByIdDoc(Path pathObj, List<Field> fields) {
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("method", "GET");
		map.put("path", pathObj.getPath());
		map.put("description", "Get record by ID");
		Map responseFields = JsonUtil.parseJson("{'field_name':'_id','field_type':'String','required':'否','example':''}", Map.class);
		map.put("requestFields", "{'field_name':'id','field_type':'String','required':'是','example':'5edf26e662a932388458f153'}");
		map.put("responseFields", responseFields);
		map.put("requestExample", "http://127.0.0.1:3080" + pathObj.getPath());
		map.put("responseExample", getExample((List<Field>) map.get("responseFields")));
		list.add(map);
		return list;
	}

	public List<Map<String, Object>> custGetDoc(Path pathObj, List<Field> fields) {
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("method", "GET");
		map.put("path", pathObj.getPath());
		map.put("description", "Paging records");
		map.put("requestFields", getFileds(fields, (Path) pathObj.getAvailableQueryField(), (Path) pathObj.getRequiredQueryField()));
		map.put("responseFields", getFileds(pathObj.getFields(), (Path) pathObj.getAvailableQueryField(), (Path) pathObj.getRequiredQueryField()));
		map.put("requestExample", "URL:http://127.0.0.1:3080" + pathObj.getPath() + "?filter={\"limit\":10,\"skip\"=50，\"where\":{\"property\":{\"operator\":value}}}");
		map.put("responseExample", "[\\n\" +\n" +
				"\t\t\tgetExample(obj.responseFields)+\n" +
				"\t\t\t\"]");
		list.add(map);
		Map<String, Object> map1 = new HashMap<>();
		map1.put("method", "POST");
		map1.put("path", pathObj.getPath());
		map1.put("description", "Paging records");
		map1.put("requestFields", getFileds(fields, (Path) pathObj.getAvailableQueryField(), (Path) pathObj.getRequiredQueryField()));
		map1.put("requestExample", "URL:http://127.0.0.1:3080" + pathObj.getPath() + "\\nbody:\\n{filter:{\"limit\":10,\"skip\"=50，\"where\":{\"property\":{\"operator\":value}}}}");
		map1.put("responseExample", "[\\n\" +\n" +
				"\t\t\tgetExample(obj1.responseFields)+\n" +
				"\t\t\t\"]");
		list.add(map1);
		return list;
	}

	public List<Map<String, Object>> findPageDoc(Path pathObj, List<Field> fields) {
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("method", "GET");
		map.put("path", pathObj.getPath());
		map.put("description", "Paging records");
		map.put("requestFields", "{'field_name':'limit','field_type':'int','required':'是','example':'10'},{'field_name':'skip','field_type':'int','required':'是','example':'50'},\n" +
				"\t\t\t{'field_name':'where','field_type':'Object','required':'否','example':'{\"where\":{\"property\":value}}'");
		Map responseFields = JsonUtil.parseJson("{'field_name':'_id','field_type':'String','required':'否','example':''}", Map.class);
		map.put("responseFields", responseFields);
		map.put("requestExample", "URL:http://127.0.0.1:3080" + pathObj.getPath() + "/find?filter={\"limit\":10,\"skip\"=50，\"where\":{\"property\":{\"operator\":value}}}");
		map.put("responseExample", "[\\n\" +\n" +
				"\t\t\tgetExample(obj.responseFields)+\n" +
				"\t\t\t\"]\"");
		list.add(map);
		Map<String, Object> map1 = new HashMap<>();
		map1.put("method", "POST");
		map1.put("path", pathObj.getPath() + "/find");
		map1.put("description", "Paging records");
		map1.put("requestFields", "{'field_name':'limit','field_type':'int','required':'是','example':'10'},\n" +
				"\t\t\t{'field_name':'skip','field_type':'int','required':'是','example':'50'},\n" +
				"\t\t\t{'field_name':'where','field_type':'Object','required':'否','example':'{\"where\":{\"property\":value}}'}");
		map1.put("responseFields", responseFields);
		map1.put("requestExample", "URL:http://127.0.0.1:3080" + pathObj.getPath() + "\\nbody:\\n{filter:{\"limit\":10,\"skip\"=50，\"where\":{\"property\":{\"operator\":value}}}}");
		map1.put("responseExample", "[\\n\" +\n" +
				"\t\t\tgetExample(obj1.responseFields)+\n" +
				"\t\t\t\"]");
		list.add(map1);
		return list;
	}

	public List<Map<String, Object>> updateByIdDoc(Path pathObj, List<Field> fields) {
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("method", "POST");
		map.put("path", pathObj.getPath());
		map.put("description", "Update by ID");
		map.put("requestFields", getFileds(fields, null, null));
		map.put("responseFields", new ArrayList<>());
		map.put("requestExample", "URL:http://127.0.0.1:3080" + pathObj.getPath() + "\n" + "Body:\n" + getExample((List<Field>) map.get("requestFields")));
		map.put("responseExample", "{\n" + "'204':{\n" + "description: 'CAR_CUSTOMER PATCH success',\n" + "},\n" + "}");
		list.add(map);
		Map<String, Object> map1 = new HashMap<>();
		map1.put("method", "PATCH");
		map1.put("path", pathObj.getPath());
		map1.put("description", "Update by ID");
		map1.put("requestFields", getFileds(fields, null, null));
		map1.put("responseFields", new ArrayList<>());
		map1.put("requestExample", "URL:http://127.0.0.1:3080" + pathObj.getPath() + "\n" + "Body:\n" + getExample((List<Field>) map1.get("requestFields")));
		map1.put("responseExample", "{\n" + "'204':{\n" + "description: 'CAR_CUSTOMER PATCH success',\n" + "},\n" + "}");
		list.add(map1);
		Map<String, Object> map2 = new HashMap<>();
		map2.put("method", "PATCH");
		map2.put("path", pathObj.getPath().split("\\{", 0));
		map2.put("description", "update all match document with where");
		map2.put("requestFields", getFileds(fields, null, null));
		map2.put("responseFields", new ArrayList<>());
		map2.put("requestExample", "http://127.0.0.1:3080" + pathObj.getPath().split("\\{", 0) + "?filter={\"where\":{\"property\":value\")\n" + "Body:\n" + getExample((List<Field>) map1.get("requestFields")));
		map2.put("responseExample", "{\n" +
				"            '204': {\n" +
				"              description: 'CAR_CUSTOMER PATCH success',\n" +
				"            },\n" +
				"}");
		list.add(map2);
		return list;
	}

	public List<Map<String, Object>> deleteByIdDoc(Path pathObj, List<Field> fields) {
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("method", "GET");
		map.put("path", pathObj.getPath() + "/delete");
		map.put("description", "Delete by ID");
		map.put("requestFields", "{'field_name':'id','field_type':'String','required':'是','example':'5edf26e662a932388458f153'}");
		map.put("responseFields", new ArrayList<>());
		map.put("requestExample", "http://127.0.0.1:3080" + pathObj.getPath());
		map.put("responseExample", "{\n" +
				"            '204': {\n" +
				"              description: 'CAR_CUSTOMER DELETE success',\n" +
				"            },\n" +
				"}");
		list.add(map);
		Map<String, Object> map1 = new HashMap<>();
		map1.put("method", "DELETE");
		map1.put("path", pathObj.getPath());
		map1.put("description", "Delete all match document with where");
		map1.put("requestFields", "{'field_name':'id','field_type':'String','required':'是','example':'5edf26e662a932388458f153'}");
		map1.put("responseFields", new ArrayList<>());
		map1.put("requestExample", "http://127.0.0.1:3080" + pathObj.getPath());
		map1.put("responseExample", "{\n" +
				"            '204': {\n" +
				"              description: 'CAR_CUSTOMER DELETE success',\n" +
				"            },\n" +
				"}");
		list.add(map1);
		Map<String, Object> map2 = new HashMap<>();
		map2.put("method", "DELETE");
		map2.put("path", pathObj.getPath().split("\\{", 0));
		map2.put("description", "Delete all match document with where");
		map2.put("requestFields", "{'field_name':'where','field_type':'object','required':'是','example':''}");
		map2.put("responseFields", new ArrayList<>());
		map2.put("requestExample", "http://127.0.0.1:3080" + pathObj.getPath().split("\\{", 0) + "?filter={\"where\":{\"property\":value\")");
		map2.put("responseExample", "{\n" +
				"            '204': {\n" +
				"              description: 'CAR_CUSTOMER DELETE success',\n" +
				"            },\n" +
				"}");
		list.add(map2);
		return list;
	}


	public List<Field> getFileds(List<Field> fields, Path availableQueryField, Path requiredQueryField) {
		List obj = availableQueryField != null ? (List) availableQueryField : new ArrayList<>();
		List obj1 = requiredQueryField != null ? (List) requiredQueryField : new ArrayList<>();
		List<Field> newField = new ArrayList<>();
		for (Field field : fields) {
			if (!Objects.equals(field.getFieldName(), "id") && (field.getVisible() == null || field.getVisible()) && obj.size() == 0 || obj.contains(field.getFieldName())) {
				String required = obj1.contains(field.getFieldName()) ? "是" : "否";
				Field field1 = new Field();
        /*map.put("field_name",field.getFieldName());
        map.put("field_type",StringUtils.isEmpty(field.getJavaType())||StringUtils.isEmpty(field.getField_type()));
        map.put("required",required);
        map.put("example","");*/
				field1.setFieldName(field.getFieldName());
				field1.setField_type(StringUtils.isNotBlank(field.getJavaType()) ? field.getJavaType() : field.getField_type());
				field1.setRequired(required);
				field1.setExample("");
				newField.add(field1);
			}
		}
		return newField;
	}


	public List<Map<String, Object>> createApiDoc(Path pathObj, List<Field> fields) {
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("method", "POST");
		map.put("path", pathObj.getPath());
		map.put("description", "Use this interface to create new data");
		map.put("requestFields", fields);
		Map responseFields = JsonUtil.parseJson("{'field_name':'_id','field_type':'String','required':'否','example':''}", Map.class);
		map.put("responseFields", responseFields);
		map.put("requestExample", getExample(fields));
		map.put("responseExample", getExample(fields));
		list.add(map);
		Map<String, Object> map1 = new HashMap<>();
		map1.put("method", "POST");
		map1.put("path", pathObj.getPath() + "/batch");
		map1.put("description", "Batch import excel records");
		Map<String, Object> objectMap = new HashMap<>();
		objectMap.put("field_name", "total");
		objectMap.put("field_type", "int");
		objectMap.put("required", "否");
		objectMap.put("example", "0");
		List<Map<String, Object>> object = new ArrayList<>();
		map.put("responseFields", object.add(objectMap));
		map1.put("requestExample", "use multipart/form-data upload xlsx file.");
		map1.put("responseExample", "{\n' + '  'total': 0,\n' + '}");
		list.add(map1);
		Map<String, Object> map2 = new HashMap<>();
		map2.put("method", "GET");
		map2.put("path", pathObj.getPath() + "/batch");
		map2.put("description", "Download batch import excel template");
		map2.put("requestFields", new ArrayList<>());
		map2.put("responseFields", new ArrayList<>());
		map2.put("requestExample", "Parameters\nNo parameters");
		map2.put("responseExample", "content: {'application/octet-stream': {}}");
		list.add(map2);
		return list;
	}

	public String getExample(List<Field> fields) {
		Map<String, Object> map = new HashMap<>();
		String str = "{";
		for (Field field : fields) {
			map.put(field.getFieldName(), "");
			str = str + "\n " + field.getFieldName() + " : " + field.getField_type() + ",";
		}
		str = str.substring(0, str.length() - 1);
		str = str + "}";
		return str;
	}


	public List findAllActiveApi(ModuleStatusEnum moduleStatusEnum) {
		Query query = Query.query(Criteria.where("status").is(moduleStatusEnum.getValue()).and("is_deleted").ne(true));
		List<ModulesDto> modulesDtoList = findAll(query);
		return modulesDtoList;
	}

	/**
	 * basePath+version 不能重复
	 */
	private List<ModulesDto> isBasePathAndVersionRepeat(String basepath, String apiVersion) {
		Query query = Query.query(Criteria.where("is_deleted").ne(true));
		query.addCriteria(Criteria.where("basePath").is(basepath));
		query.addCriteria(Criteria.where("apiVersion").is(apiVersion));
		List<ModulesDto> modulesDto = findAll(query);
		return modulesDto;
	}

	public PreviewVo preview(UserDetail userDetail) {
		PreviewVo previewVo = new PreviewVo();
		if (null == userDetail) {
			return previewVo;
		}
		String userId = userDetail.getUserId();
		if (StringUtils.isBlank(userId)) {
			return previewVo;
		}
		ApiCallStatsDto apiCallStatsDto = apiCallStatsService.aggregateByUserId(userDetail.isRoot() ? "" : userId);
		Query query = Query.query(Criteria.where("is_deleted").ne(true));
		if (!userDetail.isRoot()) {
			query.addCriteria(Criteria.where("user_id").is(userId));
		}
		previewVo.setTotalCount(count(query));
		previewVo.setVisitTotalCount(apiCallStatsDto.getCallTotalCount());
		previewVo.setTransmitTotal(apiCallStatsDto.getTransferDataTotalBytes());
		previewVo.setWarningApiCount(apiCallStatsDto.getAlarmApiTotalCount());
		previewVo.setWarningVisitTotalCount(apiCallStatsDto.getCallAlarmTotalCount());
		previewVo.setVisitTotalLine(apiCallStatsDto.getResponseDataRowTotalCount());
		previewVo.setLastUpdAt(null != apiCallStatsDto.getLastUpdAt() ? apiCallStatsDto.getLastUpdAt().getTime() : 0L);
		return previewVo;
	}

	public List<ModulesDto> getByUserId(String userId) {
		Query query = Query.query(Criteria.where("is_deleted").ne(true).and("user_id").is(userId));
		List<ModulesDto> modulesDtoList = findAll(query);
		return modulesDtoList;
	}


    /**
     * type
     * failRate  失败排行榜   ；
     * latency     响应时间排行榜
     *
     * @param filter
     * @return
     */
    public RankListsVo rankLists(Filter filter, UserDetail userDetail) {
        String order = (String) (filter.getOrder() == null ? "DESC" : filter.getOrder());
        String type = (String) filter.getWhere().getOrDefault("type", "failRate");
		switch (type) {
			case "responseTime":
				type = "maxResponseTime";
				break;
			case "failRate":
				type = "accessFailureRate";
				break;
			default:
				break;
		}
        RankListsVo rankListsVo = new RankListsVo();

		Query query = new Query();
		if (!userDetail.isRoot()) {
			query.addCriteria(Criteria.where("user_id").is(userDetail.getUserId()));
		}
		Long total = apiCallStatsService.count(query);
		query.fields().include("moduleId", type);
		if ("DESC".equals(order)) {
			query.with(Sort.by(type).descending());

		} else if ("ASC".equals(order)) {
			query.with(Sort.by(type).ascending());
		}
		TmPageable tmPageable = filterToTmPageable(filter);
		List<ApiCallStatsDto> apiCallStatsDtoList = apiCallStatsService.findAll(query.with(tmPageable));
		List<ObjectId> moduleIds = apiCallStatsDtoList.stream().map(acs -> new ObjectId(acs.getModuleId())).collect(Collectors.toList());
		List<ModulesDto> modulesDtoList = findAll(Query.query(Criteria.where("id").in(moduleIds)));

		List<Map<String, Object>> list = new ArrayList<>();
		String finalType = type;
		apiCallStatsDtoList.forEach(apiCallStatsDto -> {
			Map<String, Object> map = new HashMap<>();
			Object value = BeanUtil.getProperty(apiCallStatsDto, finalType) == null ? 0L : BeanUtil.getProperty(apiCallStatsDto, finalType);
			AtomicReference<String> name = new AtomicReference<>("");
			modulesDtoList.stream().filter(modulesDto -> apiCallStatsDto.getModuleId().equals(modulesDto.getId().toString())).findFirst().ifPresent(modulesDto -> name.set(modulesDto.getName()));
			map.put(name.get(), value);
			list.add(map);
		});

		rankListsVo.setItems(list);
		rankListsVo.setTotal(total);
		return rankListsVo;
	}


	public Page<ApiListVo> apiList(Filter filter, UserDetail userDetail) {
		Page<ApiListVo> page = new Page<>(0L, new ArrayList<>());
		String apiCallStatsCollectionName;
		try {
			apiCallStatsCollectionName = EntityUtils.documentAnnotationValue(ApiCallStatsEntity.class);
		} catch (Exception e) {
			throw new BizException("Get ApiCallStatsEntity's collection name failed", e);
		}
		MongoCollection<Document> apiCallStatsCollection = repository.getMongoOperations().getCollection(apiCallStatsCollectionName);
		List<Document> pipeline = new ArrayList<>();
		pipeline.add(new Document("$project", new Document("moduleId", new Document("$toObjectId", "$moduleId"))
				.append("responseDataRowTotalCount", 1)
				.append("callTotalCount", 1)
				.append("transferDataTotalBytes", 1)
				.append("clientIds", 1)
		));
		pipeline.add(new Document("$lookup", new Document("from", "Modules")
				.append("let", new Document("moduleId", "$moduleId"))
				.append("pipeline", Arrays.asList(
						new Document("$project", new Document("_id", 1).append("name", 1).append("status", 1).append("user_id", 1).append("createTime", 1)),
						new Document("$match", new Document("$expr", new Document("$eq", Arrays.asList("$_id", "$$moduleId"))))
				))
				.append("as", "modules")
		));
		pipeline.add(new Document("$unwind", new Document("path", "$modules")));
		Where where = filter.getWhere();
		Document match = new Document();
		if (!userDetail.isRoot()) {
			match.append("modules.user_id", userDetail.getUserId());
		}
		where.forEach((k, v) -> {
			if ("clientId".equals(k)) {
				match.append("clientIds", v);
			} else {
				match.append("modules." + k, v);
			}
		});
		if (!match.isEmpty()) {
			pipeline.add(new Document("$match", match));
		}
		List<Document> itemPipeline = new ArrayList<>();
		Object order = filter.getOrder();
		Document sortDoc = new Document();
		if (order instanceof String) {
			String[] split1 = ((String) order).split(",");
			for (String s : split1) {
				String[] split2 = s.trim().split(" ");
				if (split2.length != 2) {
					continue;
				}
				sortDoc.put("modules." + split2[0], "ASC".equals(split2[1]) ? 1 : -1);
			}
		}
		if (!sortDoc.isEmpty()) {
			itemPipeline.add(new Document("$sort", sortDoc));
		}
		int skip = filter.getSkip();
		if (skip > 0) {
			itemPipeline.add(new Document("$skip", skip));
		}
		int limit = filter.getLimit();
		if (limit > 0) {
			itemPipeline.add(new Document("$limit", limit));
		}
		Document facet = new Document("total", Arrays.asList(new Document("$group", new Document("_id", null).append("total", new Document("$sum", 1)))))
				.append("items", itemPipeline);
		pipeline.add(new Document("$facet", facet));
		long startMs = System.currentTimeMillis();
		try (
				MongoCursor<Document> iterator = apiCallStatsCollection.aggregate(pipeline).allowDiskUse(true).iterator()
		) {
			if (iterator.hasNext()) {
				Document doc = iterator.next();
				long cost = System.currentTimeMillis() - startMs;
				if (log.isDebugEnabled()) {
					StringBuilder pipelineStr = new StringBuilder();
					pipeline.forEach(p -> pipelineStr.append(p.toJson()).append(System.lineSeparator()));
					log.debug("Execute aggregate on ApiCallStats collection, pipeline: {}, cost: {} ms, result: {}", pipelineStr, cost, doc.toJson());
				}
				List<?> tempList;
				Object total = doc.get("total");
				if (total instanceof List) {
					tempList = (List<?>) total;
					if (!tempList.isEmpty()) {
						page.setTotal(getLong((Document) tempList.get(0), "total"));
					}
				}
				Object items = doc.get("items");
				if (items instanceof List) {
					tempList = (List<?>) items;
					if (!tempList.isEmpty()) {
						for (Object o : tempList) {
							if (!(o instanceof Document)) {
								continue;
							}
							Document item = (Document) o;
							Object modules = item.get("modules");
							if (!(modules instanceof Document)) {
								continue;
							}
							ApiListVo apiListVo = new ApiListVo();
							apiListVo.setId(((Document) modules).getObjectId("_id").toString());
							apiListVo.setName(((Document)modules).getString("name"));
							apiListVo.setStatus(((Document) modules).getString("status"));
							apiListVo.setVisitLine(getLong(item, "responseDataRowTotalCount"));
							apiListVo.setVisitCount(getLong(item, "callTotalCount"));
							apiListVo.setTransitQuantity(getLong(item, "transferDataTotalBytes"));
							page.getItems().add(apiListVo);
						}
					}
				}
			}
		}
		return page;
	}

	/**
	 * 时间间隔  （可选5分钟、10分钟、30分钟、60分钟））
	 * "type":    "visitTotalLine",       可选值：visitTotalLine (访问行数),
	 * timeConsuming（耗时），
	 * speed（传输速率）
	 * latency（响应时间）
	 *
	 * @param apiDetailParam
	 * @return
	 */
	public ApiDetailVo apiDetail(ApiDetailParam apiDetailParam) {
		ApiDetailVo apiDetailVo = new ApiDetailVo();
		ApiCallStatsDto apiCallStatsDto = apiCallStatsService.findOne(Query.query(Criteria.where("moduleId").is(apiDetailParam.getId())));
		if (null != apiCallStatsDto) {
			apiDetailVo.setVisitTotalCount(apiCallStatsDto.getCallTotalCount());
			//传输的数据量
			apiDetailVo.setVisitQuantity(apiCallStatsDto.getTransferDataTotalBytes());
			// 总的响应时间
			Long totalLatency = apiCallStatsDto.getTotalResponseTime();
			// 传输速率
			if (totalLatency > 0L) {
				apiDetailVo.setSpeed(BigDecimal.valueOf(apiCallStatsDto.getTransferDataTotalBytes()).divide(BigDecimal.valueOf(totalLatency), 2, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(1000)).doubleValue());
			}
			Query lastApiCallQuery = Query.query(Criteria.where("allPathId").is(apiDetailParam.getId())).with(Sort.by("_id").descending()).limit(1);
			lastApiCallQuery.fields().include("res_rows", "latency");
			ApiCallEntity lastApiCall = apiCallService.findOne(lastApiCallQuery);
			if (null != lastApiCall && null != lastApiCall.getResRows() && lastApiCall.getResRows() > 0) {
				//最新一次的响应时间
				apiDetailVo.setResponseTime(BigDecimal.valueOf(lastApiCall.getLatency()).divide(BigDecimal.valueOf(lastApiCall.getResRows()), 2, RoundingMode.HALF_UP).doubleValue());
				//最新一次的的耗时
				apiDetailVo.setTimeConsuming(lastApiCall.getLatency());
			}
			// 总行数
			apiDetailVo.setVisitTotalLine(apiCallStatsDto.getResponseDataRowTotalCount());
			//查询客户端
			Set<String> clientIds = apiCallStatsDto.getClientIds();
			Query applicationQuery = Query.query(Criteria.where("id").in(clientIds));
			applicationQuery.fields().include("id", "name");
			List<ApplicationDto> applicationDtoList = applicationService.findAll(applicationQuery);
			applicationDtoList.forEach(applicationDto -> {
				Map<String, String> clientNameMap = new HashMap<>();
				clientNameMap.put("id", applicationDto.getId().toString());
				clientNameMap.put("name", applicationDto.getName());
				apiDetailVo.getClientNameList().add(clientNameMap);
			});
		}

		Integer guanluary = apiDetailParam.getGuanluary();

		ZonedDateTime end = ZonedDateTime.now().withSecond(0).withNano(0);
		ZonedDateTime start = end.minusMinutes(guanluary).withSecond(0).withNano(0);
		Date startDate = Date.from(start.toInstant());
		Date endDate = Date.from(end.toInstant());
		Query query = Query.query(Criteria.where("moduleId").is(apiDetailParam.getId())
						.andOperator(
								Criteria.where("apiCallTime").gte(startDate),
								Criteria.where("apiCallTime").lte(endDate)
						))
				.with(Sort.by("apiCallTime").ascending());
		List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = apiCallMinuteStatsService.findAll(query);
		IntStream.range(0, guanluary).forEach(i -> {
			long pointTime = startDate.getTime() + i * oneMinuteMillSeconds;
			apiDetailVo.getTime().add(pointTime);
			ApiCallMinuteStatsDto pointStatsDto = apiCallMinuteStatsDtoList.stream().filter(apiCallMinuteStatsDto -> apiCallMinuteStatsDto.getApiCallTime().getTime() == pointTime).findFirst().orElse(null);
			if (null != pointStatsDto) {
				apiDetailVo.getValue().add(pickValue(apiDetailParam, pointStatsDto));
			} else {
				apiDetailVo.getValue().add(0);
			}
		});
		return apiDetailVo;
	}

	public Object pickValue(ApiDetailParam apiDetailParam, ApiCallMinuteStatsDto apiCallMinuteStatsDto) {
		Object value = 0;
		if ("latency".equals(apiDetailParam.getType())) {
			value = apiCallMinuteStatsDto.getTotalResponseTime();
		} else if ("visitTotalLine".equals(apiDetailParam.getType())) {
			value = apiCallMinuteStatsDto.getResponseDataRowTotalCount();
		} else if ("responseTime".equals(apiDetailParam.getType())) {
			value = apiCallMinuteStatsDto.getResponseTimePerRow();
		} else if ("speed".equals(apiDetailParam.getType())) {
			value = apiCallMinuteStatsDto.getTransferBytePerSecond();
		}
		return value;
	}

	public List<ModulesDto> findByConnectionId(String connectionId) {
		List<ModulesDto> modulesDtoList = findAll(Query.query(Criteria.where("connection").is(MongoUtils.toObjectId(connectionId)).and("is_deleted").ne(true)));
		return modulesDtoList;
	}


	/**
	 * 生成,compare update function，just add check function
	 *
	 * @param modulesDto
	 * @param userDetail
	 * @return
	 */
	public ModulesDto generate(ModulesDto modulesDto, UserDetail userDetail) {
		if (findByName(modulesDto.getName()).size() > 1) {
			throw new BizException("Modules.Name.Existed");
		}
		if (isBasePathAndVersionRepeat(modulesDto.getBasePath(), modulesDto.getApiVersion()).size() > 1) {
			throw new BizException("Modules.BasePathAndVersion.Existed");
		}
		checkoutInputParamIsValid(modulesDto);
		modulesDto.setStatus(ModuleStatusEnum.PENDING.getValue());
		Where where = new Where();
		where.put("id", modulesDto.getId().toString());
		return super.upsertByWhere(where, modulesDto, userDetail);
	}

	private void checkoutInputParamIsValid(ModulesDto modulesDto) {
		String apiType = modulesDto.getApiType();
		List<Path> paths = modulesDto.getPaths();
		if (StringUtils.isBlank(modulesDto.getDataSource())) throw new BizException("datasource can't be null");
		if (StringUtils.isBlank(modulesDto.getTableName())) throw new BizException("tableName can't be null");
		if (StringUtils.isBlank(modulesDto.getApiType())) throw new BizException("apiType can't be null");
		if (StringUtils.isBlank(modulesDto.getConnectionId())) throw new BizException("connectionId can't be null");
		if (StringUtils.isBlank(modulesDto.getOperationType())) throw new BizException("operationType can't be null");
		if (StringUtils.isBlank(modulesDto.getConnectionType())) throw new BizException("connectionType can't be null");
		if (StringUtils.isBlank(modulesDto.getConnectionName())) throw new BizException("connectionName can't be null");
		if (CollectionUtils.isNotEmpty(paths)) {
			for (Path path : paths) {
				//base param can't be null
				List<Param> params = path.getParams();
				if (CollectionUtils.isEmpty(params))
					throw new BizException("params can't be null");
				Map<String, List<Param>> paramMap = params.stream().collect(Collectors.groupingBy(Param::getName));
				List<Param> pages = paramMap.get("page");
				List<Param> limits = paramMap.get("limit");
				List<Param> filters = paramMap.get("filter");
				if (ApiTypeEnum.DEFAULT_API.getValue().equals(apiType) || ApiTypeEnum.CUSTOMER_QUERY.getValue().equals(apiType)) {
					if (CollectionUtils.isEmpty(pages))
						throw new BizException("paths's page can't be null");
					if (CollectionUtils.isEmpty(limits))
						throw new BizException("paths's limit can't be null");
					if (ApiTypeEnum.DEFAULT_API.getValue().equals(apiType) && CollectionUtils.isEmpty(filters))
						throw new BizException("paths's filter can't be null");
				}
				//input params type checkout
				for (Param param : params) {
					if (ApiTypeEnum.DEFAULT_API.getValue().equals(apiType) && StringUtils.isNotBlank(param.getType()) && "object".equalsIgnoreCase(param.getType().trim()))
						continue;
					if (!ParamTypeEnum.isValid(param.getType(), param.getDefaultvalue()))
						throw new BizException(param.getName() + " is invalid");
				}
			}
		}
	}

	public void batchLoadTask(HttpServletResponse response, List<String> ids, UserDetail user) {

		List<TaskUpAndLoadDto> jsonList = new ArrayList<>();
		List<ModulesDto> allModules = findAllModulesByIds(ids);
		Map<String, ModulesDto> modulesDtoMap = allModules.stream().collect(Collectors.toMap(t -> t.getId().toHexString(), Function.identity(), (e1, e2) -> e1));
		for (String id : ids) {
			ModulesDto modulesDto = modulesDtoMap.get(id);
			if (null != modulesDto) {
				modulesDto.setCreateUser(null);
				modulesDto.setCustomId(null);
				modulesDto.setLastUpdBy(null);
				modulesDto.setUserId(null);
				modulesDto.setListtags(null);
				jsonList.add(new TaskUpAndLoadDto("Modules", JsonUtil.toJsonUseJackson(modulesDto)));

				DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(modulesDto.getConnectionId()));
				dataSourceConnectionDto.setCreateUser(null);
				dataSourceConnectionDto.setCustomId(null);
				dataSourceConnectionDto.setLastUpdBy(null);
				dataSourceConnectionDto.setUserId(null);
				dataSourceConnectionDto.setListtags(null);
				String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", dataSourceConnectionDto, null);
				MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
						Query.query(Criteria.where("qualified_name").is(databaseQualifiedName).and("is_deleted").ne(true)), user);
				jsonList.add(new TaskUpAndLoadDto("MetadataInstances", JsonUtil.toJsonUseJackson(dataSourceMetadataInstance)));
				jsonList.add(new TaskUpAndLoadDto("Connections", JsonUtil.toJsonUseJackson(dataSourceConnectionDto)));
			}
		}
		String json = JsonUtil.toJsonUseJackson(jsonList);

		AtomicReference<String> fileName = new AtomicReference<>("");
		String yyyymmdd = DateUtil.today().replaceAll("-", "");
		FunctionUtils.isTureOrFalse(ids.size() > 1).trueOrFalseHandle(
				() -> fileName.set("module_batch" + "-" + yyyymmdd),
				() -> fileName.set(modulesDtoMap.get(ids.get(0)).getName() + "-" + yyyymmdd)
		);
		fileService.viewImg1(json, response, fileName.get() + ".json.gz");
	}

	public void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover) {

		if (!Objects.requireNonNull(multipartFile.getOriginalFilename()).endsWith("json.gz")) {
			//不支持其他的格式文件
			throw new BizException("Modules.ImportFormatError");
		}
		try {
			byte[] bytes = GZIPUtil.unGzip(multipartFile.getBytes());

			String json = new String(bytes, StandardCharsets.UTF_8);

			List<ModulesUpAndLoadDto> modulesUpAndLoadDtos = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<ModulesUpAndLoadDto>>() {
			});

			if (modulesUpAndLoadDtos == null) {
				//不支持其他的格式文件
				throw new BizException("Modules.ImportFormatError");
			}

			List<MetadataInstancesDto> metadataInstancess = new ArrayList<>();
			List<ModulesDto> modulesDtos = new ArrayList<>();
			List<DataSourceConnectionDto> connections = new ArrayList<>();
			for (ModulesUpAndLoadDto modulesUpAndLoadDto : modulesUpAndLoadDtos) {
				try {
					String dtoJson = modulesUpAndLoadDto.getJson();
					if (org.apache.commons.lang3.StringUtils.isBlank(modulesUpAndLoadDto.getJson())) {
						continue;
					}
					if ("MetadataInstances".equals(modulesUpAndLoadDto.getCollectionName())) {
						metadataInstancess.add(JsonUtil.parseJsonUseJackson(dtoJson, MetadataInstancesDto.class));
					} else if ("Modules".equals(modulesUpAndLoadDto.getCollectionName())) {
						ModulesDto modulesDto = JsonUtil.parseJsonUseJackson(dtoJson, ModulesDto.class);
						checkoutInputParamIsValid(modulesDto);
						modulesDto.setConnection(MongoUtils.toObjectId(modulesDto.getConnectionId()));
						modulesDtos.add(modulesDto);
					} else if ("Connections".equals(modulesUpAndLoadDto.getCollectionName())) {
						connections.add(JsonUtil.parseJsonUseJackson(dtoJson, DataSourceConnectionDto.class));
					}
				} catch (Exception e) {
					log.error("error", e);
				}
			}

			Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
			Map<String, MetadataInstancesDto> metaMap = new HashMap<>();
			try {
				conMap = dataSourceService.batchImport(connections, user, cover);
				metaMap = metadataInstancesService.batchImport(metadataInstancess, user, cover, conMap);
			} catch (Exception e) {
				log.error("metadataInstancesService.batchImport error", e);
			}
			try {
				batchImport(modulesDtos, user, cover, conMap, metaMap);
			} catch (Exception e) {
				log.error("Modules.batchImport error", e);
			}

		} catch (Exception e) {
			//e.printStackTrace();
			//不支持其他的格式文件
			throw new BizException("Modules.ImportFormatError");
		}

	}

	private void batchImport(List<ModulesDto> modulesDtos, UserDetail user, boolean cover, Map<String, DataSourceConnectionDto> conMap, Map<String, MetadataInstancesDto> metaMap) {


		for (ModulesDto modulesDto : modulesDtos) {
			Query query = new Query(Criteria.where("_id").is(modulesDto.getId()).and("is_deleted").ne(true));
			query.fields().include("_id", "user_id");
			ModulesDto one = findOne(query, user);

			modulesDto.setIsDeleted(false);
			modulesDto.setStatus(ModuleStatusEnum.PENDING.getValue());

			if (one == null) {
				ModulesDto one1 = findOne(new Query(Criteria.where("_id").is(modulesDto.getId()).and("is_deleted").ne(true)));
				if (one1 != null) {
					modulesDto.setId(null);
				}
			}

			if (one == null || cover) {
				ObjectId objectId = null;
				if (one != null) {
					objectId = one.getId();
				}

				while (checkTaskNameNotError(modulesDto.getName(), user, objectId)) {
					modulesDto.setName(modulesDto.getName() + "_import");
				}

				if (one == null) {
					if (modulesDto.getId() == null) {
						modulesDto.setId(new ObjectId());
					}
					ModulesEntity importEntity = repository.importEntity(convertToEntity(ModulesEntity.class, modulesDto), user);
					log.info("import api modules {}", importEntity);
				} else {
					updateByWhere(new Query(Criteria.where("_id").is(objectId)), modulesDto, user);
				}
			}
		}
	}

	private boolean checkTaskNameNotError(String newName, UserDetail user, ObjectId id) {
		Criteria criteria = Criteria.where("name").is(newName).and("is_deleted").ne(true);
		if (id != null) {
			criteria.and("_id").ne(id);
		}
		Query query = new Query(criteria);
		long count = count(query, user);
		return count > 0;
	}

	public List<ModulesDto> findAllModulesByIds(List<String> list) {
		List<ObjectId> ids = list.stream().map(ObjectId::new).collect(Collectors.toList());
		Query query = new Query(Criteria.where("_id").in(ids));
		List<ModulesEntity> entityList = findAllEntity(query);
		return CglibUtil.copyList(entityList, ModulesDto::new);
	}

	public void updateOutParameter(String id, DiscoveryFieldDto discoveryFieldDto, UserDetail userDetail) {
		if (org.springframework.util.StringUtils.isEmpty(id)) {
			throw new BizException("IllegalArgument", "Id");
		}
		Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(id));
		Query query = new Query(criteria);
		Update update = new Update().set("paths.$[].fields.$[id1].description", discoveryFieldDto.getBusinessDesc())
				.filterArray("id1.id", discoveryFieldDto.getId());
		update(query, update, userDetail);
	}

	public void updateIntParameter(String id, Param param, UserDetail userDetail) {
		if (org.springframework.util.StringUtils.isEmpty(id)) {
			throw new BizException("IllegalArgument", "Id");
		}
		Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(id));
		Query query = new Query(criteria);
		Update update = new Update().set("paths.$[].params.$[id1].description", param.getDescription())
				.filterArray("id1.name", param.getName());
		update(query, update, userDetail);
	}

	public void saveWord(HttpServletResponse response, List<String> ids, String ip, UserDetail user) {
		if (CollectionUtils.isEmpty(ids) || StringUtils.isBlank(ip)) {
			return;
		}
		List<ModulesDto> allModules = findAllModulesByIds(ids);
		Map<String, List<ModulesDto>> modules = allModules.stream().collect(Collectors.groupingBy(modulesDto -> {
			return modulesDto.getListtags().get(0).getValue();
		}));
		ApiView apiView = ApiViewUtil.convert(modules, ip);
		apiView.getApiTypeList();
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("template/testTemplate.docx");
		if (is == null) {
			log.error("未找到模板文件");
		}
		LoopRowTableRenderPolicy hackLoopTableRenderPolicy = new LoopRowTableRenderPolicy();
		Configure config = Configure.builder()
				.bind("params", hackLoopTableRenderPolicy)
				.bind("fields", hackLoopTableRenderPolicy)
				.bind("TOC", new TOCRenderPolicy())
				.bind("code", new HighlightRenderPolicy())
				.useSpringEL()
				.build();
		XWPFTemplate template = null;
		try {
			template = XWPFTemplate.compile(is, config)
					.render(apiView);
		} catch (Exception e) {
			log.error("render template error{}", e);
		}
		AtomicReference<String> fileName = new AtomicReference<>("");
		String yyyymmdd = DateUtil.today().replaceAll("-", "");
		FunctionUtils.isTureOrFalse(ids.size() > 1).trueOrFalseHandle(
				() -> fileName.set("api_word" + "-" + yyyymmdd),
				() -> fileName.set(allModules.get(0).getName() + "-" + yyyymmdd)
		);
		fileService.viewWord(template, response, fileName.get());
	}

	public void updatePermissions(ModulesPermissionsDto permissions, UserDetail userDetail) {
		if (CollectionUtils.isEmpty(permissions.getAcl())) throw new BizException("Modules.Permission.Scope.Null");
		Update update = new Update();
		update.set("paths.$[].acl", permissions.getAcl());
		updateById(permissions.getModuleId(), update, userDetail);
	}

	public void updateTags(ModulesTagsDto modulesTagsDto, UserDetail userDetail) {
		if (CollectionUtils.isEmpty(modulesTagsDto.getListtags())) throw new BizException("Modules.Tags.Null");
		Update update = new Update();
		update.set("listtags", modulesTagsDto.getListtags());
		updateById(modulesTagsDto.getModuleId(), update, userDetail);
	}
}
