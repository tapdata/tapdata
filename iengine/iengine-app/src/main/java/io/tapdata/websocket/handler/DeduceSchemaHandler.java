package io.tapdata.websocket.handler;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.utils.GZIPUtil;
import com.tapdata.tm.commons.base.convert.DagDeserialize;
import com.tapdata.tm.commons.base.convert.DagSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.MetadataTransformerDto;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.common.DAGDataEngineServiceImpl;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@EventHandlerAnnotation(type = "deduceSchema")
public class DeduceSchemaHandler implements WebSocketEventHandler<WebSocketEventResult> {

	private final static Logger logger = LogManager.getLogger(DeduceSchemaHandler.class);

	private ClientMongoOperator clientMongoOperator;

	private TaskService<TaskDto> taskService;

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void initialize(TaskService<TaskDto> taskService, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.initialize(clientMongoOperator, settingService);
		this.taskService = taskService;
	}

	@Override
	public WebSocketEventResult handle(Map event) {
		String data = (String) event.get("data");
		byte[] decode = Base64.getDecoder().decode(data);
		byte[] bytes = GZIPUtil.unGzip(decode);
		String json = new String(bytes, StandardCharsets.UTF_8);
		DeduceSchemaRequest request = JsonUtil.parseJsonUseJackson(json, DeduceSchemaRequest.class);
		DAG dagAgo = request.getTaskDto().getDag();
		request.getTaskDto().setDag(dagAgo);
		DAGDataServiceImpl dagDataService = new DAGDataEngineServiceImpl(
				request,
				taskService,
				clientMongoOperator
		);
		long start = System.currentTimeMillis();
		Map<String, List<Message>> transformSchema = request.getTaskDto().getDag().transformSchema(null, dagDataService, request.getOptions());
		logger.info("transformed cons={}", System.currentTimeMillis() - start + "ms");
		dagDataService.uploadModel(transformSchema);

		return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.DEDUCE_SCHEMA, true);
	}

	public static class DeduceSchemaRequest {

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


	@Data
	public static class UpdateTaskDagDto {

		@JsonSerialize(using = ObjectIdSerialize.class)
		@JsonDeserialize(using = ObjectIdDeserialize.class)
		private ObjectId id;

		@JsonSerialize(using = DagSerialize.class)
		@JsonDeserialize(using = DagDeserialize.class)
		private DAG dag;
	}

}
