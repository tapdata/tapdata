package io.tapdata.engine.it;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务运行所需 TM 侧数据的预置器，与 {@link TaskDtoBuilder} 配合：
 * <ol>
 *   <li>Connections 集合：源/目标连接——引擎 startTask 时
 *       {@code ConnectionUtil.getConnection(connectionId)} 按 _id 查询</li>
 *   <li>DatabaseTypes 集合：pdkHash → DatabaseType——引擎
 *       {@code ConnectionUtil.getDatabaseType} 按 pdkHash 查询</li>
 *   <li>Task/transformAllParam/{taskId}：模型推演全部参数（TransformerWsMessageDto：
 *       taskDto + options + metadataInstancesDtoList + dataSourceMap + definitionDtoMap），
 *       引擎 {@code engineTransformSchema} 经 DAGDataServiceImpl 从
 *       metadataInstancesDtoList 构建 metadataMap 加载模型（不查 TM 模型库）</li>
 * </ol>
 * <p>
 * 模型字段（metadataInstancesDtoList）必须与真实源库表结构一致——引擎/连接器按此模型
 * 读取与写入数据。冒烟级字段规格由 {@link #field(String, String, TapType, boolean)} 提供。
 */
public class TaskFixture {

	private TaskFixture() {
	}

	// ==================== 连接规格 ====================

	/** 一个连接的描述（Connections 文档 + DatabaseTypes 文档 + dataSourceMap 共用） */
	public static class ConnSpec {
		public String id;
		public String name;
		/** connection_type：source/target/source_target */
		public String connectionType;
		/** database_type：mysql/mongodb 等（Connections.database_type + DatabaseType.type） */
		public String databaseType;
		public String pdkHash;
		public String pdkType;
		/** PDK 连接配置（connector 读取的 config，如 host/port/user/password/database） */
		public Map<String, Object> config;

		public ConnSpec id(String id) {
			this.id = id;
			return this;
		}

		public ConnSpec name(String name) {
			this.name = name;
			return this;
		}

		public ConnSpec connectionType(String connectionType) {
			this.connectionType = connectionType;
			return this;
		}

		public ConnSpec databaseType(String databaseType) {
			this.databaseType = databaseType;
			return this;
		}

		public ConnSpec pdkHash(String pdkHash) {
			this.pdkHash = pdkHash;
			return this;
		}

		public ConnSpec pdkType(String pdkType) {
			this.pdkType = pdkType;
			return this;
		}

		public ConnSpec config(Map<String, Object> config) {
			this.config = config;
			return this;
		}

		/** MySQL 连接（config key 与 mysql connector 的 connection 组一致） */
		public static ConnSpec mysql(String id, String host, int port, String user, String password, String database) {
			Map<String, Object> config = new LinkedHashMap<>();
			config.put("deploymentMode", "standalone");
			config.put("host", host);
			config.put("port", port);
			config.put("database", database);
			// mysql connector 的 CommonDbConfig 字段是 user（不是 username），
			// BeanMap.putAll 按 setter 名匹配，key 不对会静默忽略导致 user=null
			config.put("user", user);
			config.put("password", password);
			return new ConnSpec()
					.id(id)
					.name(id)
					.databaseType("mysql")
					.pdkHash("mysql-pdk-hash")
					// pdkType 必须是 "pdk"：引擎 HazelcastTaskService.createNode 按
					// "pdk".equals(connection.getPdkType()) 决定走 PDK 节点（带 TaskConfig），
					// 非 pdk 走旧引擎节点（无 TaskConfig → initExternalStorage NPE）
					.pdkType("pdk")
					.config(config);
		}

		/** MongoDB 连接（config key 与 mongodb connector 的 MongodbConfig 一致） */
		public static ConnSpec mongodb(String id, String host, int port, String user, String password, String database) {
			Map<String, Object> config = new LinkedHashMap<>();
			// MongodbConfig.isUri 默认 true：getUri() 直接返回 uri 字段，空串会报 uri is blank，
			// 因此必须给完整连接串（与真实 TM 保存的 uri 一致）
			config.put("uri", "mongodb://" + host + ":" + port + "/" + database);
			config.put("host", host);
			config.put("port", port);
			config.put("database", database);
			if (StringUtils.isNotBlank(user)) {
				config.put("user", user);
				config.put("password", password);
			}
			return new ConnSpec()
					.id(id)
					.name(id)
					.databaseType("mongodb")
					.pdkHash("mongodb-pdk-hash")
					.pdkType("pdk")
					.config(config);
		}
	}

	// ==================== 预置入口 ====================

	/**
	 * 预置任务运行所需全部 TM 侧数据并下发任务。
	 *
	 * @param runtime 已启动的引擎运行时
	 * @param taskDto 任务（TaskDtoBuilder 构造）
	 * @param source  源连接规格
	 * @param target  目标连接规格
	 * @param tables  源表名列表（与任务 DAG 的 tableNames 一致，为每张表生成模型）
	 */
	public static void prepare(EngineRuntime runtime, TaskDto taskDto, ConnSpec source, ConnSpec target, List<String> tables) {
		putConnections(runtime, source, target);
		putDatabaseTypes(runtime, source, target);
		putTransformAllParam(runtime, taskDto, source, target, tables);
	}

	// ==================== Connections ====================

	private static void putConnections(EngineRuntime runtime, ConnSpec... specs) {
		List<Map<String, Object>> docs = new ArrayList<>();
		for (ConnSpec spec : specs) {
			Map<String, Object> doc = new LinkedHashMap<>();
			doc.put("_id", spec.id);
			doc.put("id", spec.id);
			doc.put("name", spec.name);
			doc.put("connection_type", spec.connectionType != null ? spec.connectionType : "source_target");
			doc.put("database_type", spec.databaseType);
			doc.put("pdkHash", spec.pdkHash);
			doc.put("pdkType", spec.pdkType);
			doc.put("config", spec.config != null ? new LinkedHashMap<>(spec.config) : new LinkedHashMap<>());
			docs.add(doc);
		}
		runtime.tm().put("Connections", docs);
	}

	// ==================== DatabaseTypes ====================

	private static void putDatabaseTypes(EngineRuntime runtime, ConnSpec... specs) {
		List<Map<String, Object>> docs = new ArrayList<>();
		for (ConnSpec spec : specs) {
			Map<String, Object> doc = new LinkedHashMap<>();
			doc.put("_id", "dbtype-" + spec.databaseType);
			doc.put("id", "dbtype-" + spec.databaseType);
			doc.put("type", spec.databaseType);
			doc.put("name", spec.databaseType);
			doc.put("pdkId", spec.databaseType);
			doc.put("pdkHash", spec.pdkHash);
			doc.put("pdkType", spec.pdkType);
			// PdkUtil.createNode/downloadPdkFileIfNeed 需要 jarFile（文件名）/jarRid（资源 id），
			// group/version 必须与 connector jar 注册信息一致：TapConnectorAnnotationHandler
			// 读 spec.json（无 group/version 时）回退到 jar MANIFEST 的
			// Implementation-Vendor/Implementation-Version（即 io.tapdata/1.0-SNAPSHOT），
			// PDKIntegration 按 pdkId@group-vversion 精确匹配，不一致报 Source not found
			doc.put("jarFile", spec.databaseType + "-connector-1.0-SNAPSHOT.jar");
			doc.put("jarRid", spec.pdkHash + "-jar");
			doc.put("group", "io.tapdata");
			doc.put("version", "1.0-SNAPSHOT");
			doc.put("buildNumber", 89);
			doc.put("scope", "public");
			doc.put("buildProfiles", Arrays.asList("DAAS", "CLOUD"));
			doc.put("supportTargetDatabaseType", Collections.singletonList(spec.databaseType));
			docs.add(doc);
		}
		runtime.tm().put("DatabaseTypes", docs);
	}

	// ==================== transformAllParam ====================

	private static void putTransformAllParam(EngineRuntime runtime, TaskDto taskDto, ConnSpec source, ConnSpec target, List<String> tables) {
		TransformerWsMessageDto wsMessageDto = new TransformerWsMessageDto();
		wsMessageDto.setTaskDto(taskDto);
		// batchNum 必须 > 0：DatabaseNode.transformSchema 用 ListUtils.partition(tables, batchNum) 分批加载模型
		DAG.Options options = new DAG.Options();
		options.setBatchNum(20);
		wsMessageDto.setOptions(options);
		Map<String, DataSourceConnectionDto> dataSourceMap = buildDataSourceMap(source, target);
		wsMessageDto.setMetadataInstancesDtoList(buildMetadataInstances(taskDto, source, target, tables, dataSourceMap));
		wsMessageDto.setDataSourceMap(dataSourceMap);
		wsMessageDto.setDefinitionDtoMap(buildDefinitionDtoMap(source, target));
		wsMessageDto.setUserId(EngineRuntime.DEFAULT_USER_ID);
		wsMessageDto.setUserName("engine-it");
		wsMessageDto.setTransformerDtoMap(new HashMap<>());

		String taskId = taskDto.getId().toHexString();
		runtime.tm().putTransformAllParam(taskId, runtime.toMap(wsMessageDto));
	}

	private static List<MetadataInstancesDto> buildMetadataInstances(TaskDto taskDto, ConnSpec source, ConnSpec target, List<String> tables,
			Map<String, DataSourceConnectionDto> dataSourceMap) {
		// 源节点 id：DAGDataEngineServiceImpl.initializeModel 按 metadataInstances.nodeId 分组
		// 把模型放入 tapTableMapHashMap（key=节点 id），HazelcastTaskService.getTapTableMap 按 node.getId() 取
		String sourceNodeId = taskDto.getDag().getNodes().stream()
				.filter(n -> n instanceof DatabaseNode || n instanceof TableNode)
				.map(com.tapdata.tm.commons.dag.Element::getId)
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("Not found source node in task dag"));
		List<MetadataInstancesDto> result = new ArrayList<>();
		// database 类型模型：createOrUpdateSchemaForDataNode 按 generateQualifiedName("database",
		// dataSource, null)="CONN_"+connId 从 metadataMap 查找，缺了会直接返回空模型列表
		// （sourceType 保持默认 SOURCE，不参与 initializeModel 的 VIRTUAL 过滤）；
		// 源/目标两个连接的 database 模型都需要（目标节点的 saveSchema 同样走该逻辑）
		for (ConnSpec spec : new ConnSpec[]{source, target}) {
			MetadataInstancesDto databaseDto = new MetadataInstancesDto();
			databaseDto.setId(new ObjectId(spec.id));
			databaseDto.setMetaType("database");
			databaseDto.setName(spec.name);
			databaseDto.setOriginalName(spec.name);
			// pdkType=pdk 时 qualifiedName 按 generatePdkQualifiedName 公式生成（与引擎侧
			// createOrUpdateSchemaForDataNode 的 metadataMap.get(...) 查找 key 保持一致）
			databaseDto.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName("database", dataSourceMap.get(spec.id), null));
			databaseDto.setConnectionId(spec.id);
			databaseDto.setHasPrimaryKey(false);
			databaseDto.setFields(new ArrayList<>());
			SourceDto dbSourceDto = new SourceDto();
			dbSourceDto.set_id(spec.id);
			dbSourceDto.setName(spec.id);
			databaseDto.setSource(dbSourceDto);
			result.add(databaseDto);
		}
		for (String table : tables) {
			MetadataInstancesDto dto = new MetadataInstancesDto();
			dto.setMetaType("table");
			dto.setName(table);
			dto.setOriginalName(table);
			dto.setQualifiedName(source.id + "." + table);
			dto.setConnectionId(source.id);
			// 引擎侧 initializeModel 只处理 sourceType=VIRTUAL 的模型（deriveSchema 引擎推导链路）
			dto.setSourceType(SourceTypeEnum.VIRTUAL.name());
			dto.setNodeId(sourceNodeId);
			dto.setHasPrimaryKey(true);
			dto.setFields(defaultTableFields());
			SourceDto sourceDto = new SourceDto();
			sourceDto.set_id(source.id);
			sourceDto.setName(source.id);
			dto.setSource(sourceDto);
			result.add(dto);
		}
		return result;
	}

	/** 冒烟级表结构：id INT 主键自增 + name VARCHAR */
	public static List<Field> defaultTableFields() {
		// tapType 必须是 TapType 的 JSON 字符串（如 {"type":8} = TapNumber、{"type":10} = TapString）：
		// PdkSchemaConvert.toPdk 用 getClassByJson 解析 type 字节再反序列化 TapType，
		// 传裸字符串（如 "INT"）会在 JsonUtil.parseJsonUseJackson 处抛 JsonParseException，
		// 导致该字段被跳过（引擎侧 nameFieldMap 为空 → CommonDbConnector.getSelectSql
		// 生成 "select `` from ..." 语法错误）
		Field id = field("id", "INT", "{\"type\":8}", true);
		id.setPrimaryKeyPosition(1);
		Field name = field("name", "VARCHAR(64)", "{\"type\":10}", false);
		return Arrays.asList(id, name);
	}

	/** 构造一个模型字段（dataType 为源库类型名，tapType 为 TapType 的 JSON 字符串） */
	public static Field field(String fieldName, String dataType, String tapType, boolean primaryKey) {
		Field field = new Field();
		field.setFieldName(fieldName);
		field.setDataType(dataType);
		field.setPureDataType(dataType);
		field.setTapType(tapType);
		field.setPrimaryKey(primaryKey);
		field.setIsNullable(!primaryKey);
		field.setDeleted(false);
		return field;
	}

	private static Map<String, DataSourceConnectionDto> buildDataSourceMap(ConnSpec... specs) {
		Map<String, DataSourceConnectionDto> map = new HashMap<>();
		for (ConnSpec spec : specs) {
			DataSourceConnectionDto dto = new DataSourceConnectionDto();
			// id 必须可转 ObjectId：generateQualifiedName 用 getId().toHexString() 拼 qualifiedName
			dto.setId(new ObjectId(spec.id));
			dto.setName(spec.name);
			dto.setConnection_type(spec.connectionType != null ? spec.connectionType : "source_target");
			dto.setDatabase_type(spec.databaseType);
			dto.setPdkHash(spec.pdkHash);
			dto.setPdkType(spec.pdkType);
			// definition* 字段必须与 definitionDtoMap 一致：引擎侧 createOrUpdateSchemaForDataNode
			// 先 setDefinition* 再用 generateQualifiedName 计算 database 模型 qualifiedName，
			// 预置侧不设置会导致两侧 key 不一致（pdk 分支走 generatePdkQualifiedName 公式）
			dto.setDefinitionPdkId(spec.pdkHash + "-pdk");
			dto.setDefinitionGroup("io.tapdata");
			dto.setDefinitionVersion("1.0.0");
			dto.setConfig(spec.config != null ? new LinkedHashMap<>(spec.config) : new LinkedHashMap<>());
			// 模型推演拼接 qualifiedName 需要 database_name（mysql）/database_uri（mongodb）
			Object database = spec.config != null ? spec.config.get("database") : null;
			if (database != null) {
				dto.setDatabase_name(String.valueOf(database));
			}
			if ("mongodb".equals(spec.databaseType) && spec.config != null) {
				dto.setDatabase_uri("mongodb://" + spec.config.get("host") + ":" + spec.config.get("port") + "/" + database);
			}
			map.put(spec.id, dto);
		}
		return map;
	}

	private static Map<String, DataSourceDefinitionDto> buildDefinitionDtoMap(ConnSpec... specs) {
		Map<String, DataSourceDefinitionDto> map = new HashMap<>();
		for (ConnSpec spec : specs) {
			DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
			dto.setPdkHash(spec.pdkHash);
			dto.setPdkType(spec.pdkType);
			dto.setPdkId(spec.pdkHash + "-pdk");
			dto.setName(spec.databaseType);
			dto.setType(spec.databaseType);
			dto.setVersion("1.0.0");
			dto.setGroup("io.tapdata");
			dto.setBuildNumber(1);
			dto.setTags(new ArrayList<>());
			// getDataSource 按 database_type 取 definitionDto（非 pdkHash）
			map.put(spec.databaseType, dto);
		}
		return map;
	}
}
