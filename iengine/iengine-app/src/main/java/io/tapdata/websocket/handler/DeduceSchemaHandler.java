package io.tapdata.websocket.handler;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastSchemaTargetNode;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;

@EventHandlerAnnotation(type = "deduceSchema")
public class DeduceSchemaHandler implements WebSocketEventHandler<WebSocketEventResult> {

	private final static Logger logger = LogManager.getLogger(DeduceSchemaHandler.class);

	private ClientMongoOperator clientMongoOperator;

	private TaskService<SubTaskDto> taskService;

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void initialize(TaskService<SubTaskDto> taskService, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.initialize(clientMongoOperator, settingService);
		this.taskService = taskService;
	}

	@Override
	public WebSocketEventResult handle(Map event) {
		DeduceSchemaRequest request = JSONUtil.map2POJO(event, DeduceSchemaRequest.class);

		DAGDataServiceImpl dagDataService = new DAGDataServiceImpl(
			request.getMetadataInstancesDtoList(),
			request.getDataSourceMap(),
			request.getDefinitionDtoMap(),
			request.getUserId(),
			request.getUserName(),
			request.getTaskDto(),
			request.getTransformerDtoMap()
		) {

			@Override
			public TapTable loadTapTable(List<Schema> schemas, String script, String nodeId, String virtualId, String customNodeId, Map<String, Object> form, SubTaskDto subTaskDto) {
				// 跑任务加载js模型
				String schemaKey = subTaskDto.getId() + "-" + virtualId;
				long startTs = System.currentTimeMillis();
				TaskClient<SubTaskDto> taskClient = taskService.startTestTask(subTaskDto);
				taskClient.join();

				logger.info("load tapTable task {} {}, cost {}ms", schemaKey, taskClient.getStatus(), (System.currentTimeMillis() - startTs));
				if (SubTaskDto.STATUS_COMPLETE.equals(taskClient.getStatus())) {
					//成功
					TapTable tapTable = HazelcastSchemaTargetNode.getTapTable(schemaKey);
					if (logger.isDebugEnabled()) {
						logger.debug("derivation results: {}", JSON.toJSONString(tapTable));
					}

					return tapTable;
				}
				return null;
			}
		};

		Map<String, List<Message>> transformSchema = request.getTaskDto().getDag().transformSchema(null, dagDataService, request.getOptions());

		TransformerWsMessageResult wsMessageResult = new TransformerWsMessageResult();
		wsMessageResult.setBatchMetadataUpdateMap(dagDataService.getBatchMetadataUpdateMap());
		wsMessageResult.setBatchInsertMetaDataList(dagDataService.getBatchInsertMetaDataList());
		wsMessageResult.setUpsertItems(dagDataService.getUpsertItems());
		wsMessageResult.setUpsertTransformer(dagDataService.getUpsertTransformer());
		wsMessageResult.setTransformSchema(transformSchema);

		//返回结果调用接口返回
		clientMongoOperator.insertOne(wsMessageResult, ConnectorConstant.TASK_COLLECTION + "/transformer/result");
		return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.DEDUCE_SCHEMA, true);
	}

	private static class DeduceSchemaRequest {

		private TaskDto taskDto;

		private DAG.Options options;

		/**
		 * 数据节点的模型
		 */
		private List<MetadataInstancesDto> metadataInstancesDtoList;

		/**
		 * 数据源的连接信息
		 */
		private Map<String, DataSourceConnectionDto> dataSourceMap;

		/**
		 * 数据源的定义信息
		 */
		private Map<String, DataSourceDefinitionDto> definitionDtoMap;

		/**
		 *
		 */
		private String userId;

		/**
		 *
		 */
		private String userName;

		/**
		 * 推演状态记录
		 */
		private Map<String, MetadataTransformerDto> transformerDtoMap;


		public TaskDto getTaskDto() {
			return taskDto;
		}

		public void setTaskDto(TaskDto taskDto) {
			this.taskDto = taskDto;
		}

		public DAG.Options getOptions() {
			return options;
		}

		public void setOptions(DAG.Options options) {
			this.options = options;
		}

		public List<MetadataInstancesDto> getMetadataInstancesDtoList() {
			return metadataInstancesDtoList;
		}

		public void setMetadataInstancesDtoList(List<MetadataInstancesDto> metadataInstancesDtoList) {
			this.metadataInstancesDtoList = metadataInstancesDtoList;
		}

		public Map<String, DataSourceConnectionDto> getDataSourceMap() {
			return dataSourceMap;
		}

		public void setDataSourceMap(Map<String, DataSourceConnectionDto> dataSourceMap) {
			this.dataSourceMap = dataSourceMap;
		}

		public Map<String, DataSourceDefinitionDto> getDefinitionDtoMap() {
			return definitionDtoMap;
		}

		public void setDefinitionDtoMap(Map<String, DataSourceDefinitionDto> definitionDtoMap) {
			this.definitionDtoMap = definitionDtoMap;
		}

		public String getUserId() {
			return userId;
		}

		public void setUserId(String userId) {
			this.userId = userId;
		}

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}


		public Map<String, MetadataTransformerDto> getTransformerDtoMap() {
			return transformerDtoMap;
		}

		public void setTransformerDtoMap(Map<String, MetadataTransformerDto> transformerDtoMap) {
			this.transformerDtoMap = transformerDtoMap;
		}
	}

	private static class DeduceSchemaResponse {

		private Map<String, Update> batchMetadataUpdateMap;
		private List<MetadataInstancesDto> batchInsertMetaDataList;
		private List<MetadataTransformerItemDto> upsertItems;
		private List<MetadataTransformerDto> upsertTransformer;

		private Map<String, List<Message>> transformSchema;

		public DeduceSchemaResponse(Map<String, Update> batchMetadataUpdateMap, List<MetadataInstancesDto> batchInsertMetaDataList, List<MetadataTransformerItemDto> upsertItems, List<MetadataTransformerDto> upsertTransformer, Map<String, List<Message>> transformSchema) {
			this.batchMetadataUpdateMap = batchMetadataUpdateMap;
			this.batchInsertMetaDataList = batchInsertMetaDataList;
			this.upsertItems = upsertItems;
			this.upsertTransformer = upsertTransformer;
			this.transformSchema = transformSchema;
		}

		public Map<String, Update> getBatchMetadataUpdateMap() {
			return batchMetadataUpdateMap;
		}

		public void setBatchMetadataUpdateMap(Map<String, Update> batchMetadataUpdateMap) {
			this.batchMetadataUpdateMap = batchMetadataUpdateMap;
		}

		public List<MetadataInstancesDto> getBatchInsertMetaDataList() {
			return batchInsertMetaDataList;
		}

		public void setBatchInsertMetaDataList(List<MetadataInstancesDto> batchInsertMetaDataList) {
			this.batchInsertMetaDataList = batchInsertMetaDataList;
		}

		public List<MetadataTransformerItemDto> getUpsertItems() {
			return upsertItems;
		}

		public void setUpsertItems(List<MetadataTransformerItemDto> upsertItems) {
			this.upsertItems = upsertItems;
		}

		public List<MetadataTransformerDto> getUpsertTransformer() {
			return upsertTransformer;
		}

		public void setUpsertTransformer(List<MetadataTransformerDto> upsertTransformer) {
			this.upsertTransformer = upsertTransformer;
		}

		public Map<String, List<Message>> getTransformSchema() {
			return transformSchema;
		}

		public void setTransformSchema(Map<String, List<Message>> transformSchema) {
			this.transformSchema = transformSchema;
		}
	}
}
