/**
 * @title: TestConnectionHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import com.alibaba.fastjson.JSONObject;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.DataSourceEnum;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.AES256Util;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@WebSocketMessageHandler(type = MessageType.TEST_CONNECTION)
@Slf4j
public class TestConnectionHandler implements WebSocketHandler {

	private static final String MONGODB_URI_PREFIX = "mongodb://";

	private final MessageQueueService messageQueueService;

	private final DataSourceService dataSourceService;

	private final DataSourceDefinitionService dataSourceDefinitionService;

	private final UserService userService;

	private final WorkerService workerService;

	public TestConnectionHandler(MessageQueueService messageQueueService, DataSourceService dataSourceService, UserService userService
			, WorkerService workerService, DataSourceDefinitionService dataSourceDefinitionService) {
		this.messageQueueService = messageQueueService;
		this.dataSourceService = dataSourceService;
		this.userService = userService;
		this.workerService = workerService;
		this.dataSourceDefinitionService = dataSourceDefinitionService;
	}
	@Override
	public void handleMessage(WebSocketContext context) throws Exception{
		MessageInfo messageInfo = context.getMessageInfo();
		messageInfo.getData().put("type", messageInfo.getType());
		messageInfo.setType("pipe");
		String userId = context.getUserId();
		if (StringUtils.isBlank(userId)){
			WebSocketManager.sendMessage(context.getSender(), WebSocketResult.fail("UserId is blank"));
			return;
		}
		Map<String, Object> data = messageInfo.getData();

		Map platformInfos = MapUtils.getAsMap(data, "platformInfo");
		List<String> tags = new ArrayList<>();
		if (MapUtils.isNotEmpty(platformInfos)){
			List<String> list = Arrays.asList("region", "zone", "agentType");
			for (Object o : platformInfos.keySet()) {
				if (list.contains(o.toString()) && platformInfos.get(o) != null){
					tags.add(platformInfos.get(o).toString());
				}
				if (platformInfos.get(o) instanceof Boolean && (Boolean)platformInfos.get(0)){
					tags.add("internet");
				}
			}
			data.put("agentTags", tags);
		}

		Object config = data.get("config");
		UserDetail userDetail = userService.loadUserById(toObjectId(userId));
		if (userDetail == null){
			WebSocketManager.sendMessage(context.getSender(), "UserDetail is null");
			return;
		}
		if (config != null) {
			String database_type = (String)data.get("database_type");
			if (StringUtils.isBlank(database_type)) {
				String pdkHash = (String) data.get("pdkHash");
				DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findByPdkHash(pdkHash, Integer.MAX_VALUE, userDetail, "type");
				database_type = definitionDto.getType();
			}
			JSONObject config1 = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(config), JSONObject.class);
			assert config1 != null;
			String uri = config1.getString("uri");
			if (StringUtils.isNotBlank(database_type) && StringUtils.isNotBlank(database_type) && database_type.toLowerCase(Locale.ROOT).contains("mongo") && StringUtils.isNotBlank(uri)) {
				if (uri.contains("******")) {
					data.put("editTest", false);
				}
			} else {
				Object password = config1.get("password");
				Object mqPassword = config1.get("mqPassword");
				if (Objects.isNull(password) && Objects.isNull(mqPassword)) {
					Object id = data.get("id");
					if (id != null) {
						DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(toObjectId(id.toString()));
						Map<String, Object> dataSourceConfig = dataSourceConnectionDto.getConfig();
						if (dataSourceConfig.containsKey("password")) {
							config1.put("password", dataSourceConfig.get("password"));
						} else if (dataSourceConfig.containsKey("mqPassword")) {
							config1.put("mqPassword", dataSourceConfig.get("mqPassword"));
						}
						data.put("config", config1);
					}
				}
			}
		}

		AtomicReference<String> receiver = new AtomicReference<>("");
		try {
			boolean accessNodeTypeEmpty = (Boolean) data.getOrDefault("accessNodeTypeEmpty", false);
			Object accessNodeType = data.get("accessNodeType");
			FunctionUtils.isTureOrFalse(accessNodeTypeEmpty ||
							AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name().equals(accessNodeType))
					.trueOrFalseHandle(() -> {
						SchedulableDto schedulableDto = new SchedulableDto();
						schedulableDto.setAgentTags(tags);
						schedulableDto.setUserId(userDetail.getUserId());
						workerService.scheduleTaskToEngine(schedulableDto, userDetail, "testConnection", "testConnection");
						receiver.set(schedulableDto.getAgentId());
					}, () -> {
						Object accessNodeProcessId = data.get("accessNodeProcessId");
						FunctionUtils.isTureOrFalse(Objects.nonNull(accessNodeProcessId)).trueOrFalseHandle(() -> {
							String processId = accessNodeProcessId.toString();
							receiver.set(processId);

							List<Worker> availableAgents = workerService.findAvailableAgentByAccessNode(userDetail, Lists.newArrayList(processId));
							if (CollectionUtils.isEmpty(availableAgents)) {
								data.put("status", "error");
								data.put("msg", "Worker " + processId + " not available, receiver is blank");
							}
						}, () -> {
							data.put("status", "error");
							data.put("msg", "Worker set error, receiver is blank");
						});
					});
		} catch (Exception e) {
			log.error("error {}", e.getMessage());
		}

		String agentId = receiver.get();
		if(StringUtils.isBlank(agentId)){
			log.warn("Receiver is blank,context: {}", JsonUtil.toJson(context));
			data.put("status", "error");
			data.put("msg", "Worker not found, receiver is blank");
		}

		if (Objects.nonNull(data.get("msg"))){
			Map<String, Object> msg = testConnectErrorData(data.get("id").toString(), data.get("msg").toString());
			context.getMessageInfo().setData(msg);

			sendMessage(context.getSender(), context);
			return;
		}

		handleData(agentId, context);
	}
		private void handleData(String receiver, WebSocketContext context) {
			Map<String, Object> data = context.getMessageInfo().getData();
			String database_type = MapUtils.getAsString(data, "database_type");
			String database_uri = MapUtils.getAsString(data, "database_uri");
			boolean containsDatabaseType = Arrays.asList("mongodb", "gridfs").contains(database_type);
			if (containsDatabaseType && StringUtils.isNotBlank(database_uri)) {
				sendMessage(receiver, context);
				return;
			}
			String id = MapUtils.getAsString(data, "id");
			Field field = new Field();
			field.put("database_password", 1);
			field.put("database_uri", 1);
			field.put("database_username", 1);
			if (StringUtils.isNotBlank(id)) {
				DataSourceConnectionDto connectionDto = dataSourceService.findById(new ObjectId(id), field);
				if (connectionDto != null) {
					Boolean justTest = MapUtils.getAsBoolean(data, "justTest");
					String database_username = MapUtils.getAsString(data, "database_username");
					String database_password = MapUtils.getAsString(data, "database_password");
					String plain_password = MapUtils.getAsString(data, "plain_password");
					if ((justTest != null && justTest && containsDatabaseType) || (StringUtils.isNotBlank(database_username) && StringUtils.isBlank(database_password)
							&& StringUtils.isBlank(connectionDto.getDatabase_name()) && StringUtils.isBlank(connectionDto.getDatabase_password()) && containsDatabaseType)) {

						// 兼容老数据，充填mongodb uri
						data.put("database_uri", connectionDto.getDatabase_uri());

						sendMessage(receiver, context);
					} else {
						if (StringUtils.isNotBlank(database_username) && StringUtils.isBlank(plain_password)) {
							// 由于脱敏，如果是编辑，前端不会传回来密码，使用从中间库查出来的密码
							data.put("database_password", connectionDto.getDatabase_password());
						} else if (StringUtils.isNotBlank(database_username) && StringUtils.isNotBlank(plain_password)) {
							data.put("database_password", AES256Util.Aes256Encode(plain_password));
							data.remove("plain_password");
						}
						Boolean isUrl = MapUtils.getAsBoolean(data, "isUrl");
						if (containsDatabaseType && (isUrl == null || !isUrl)) {
							constructURI(data);
						}

						sendMessage(receiver, context);
					}
				} else {
					data.put("status", "error");
					data.put("msg", "Connection info not found");
					sendMessage(context.getSender(), context);
				}

			} else {
				Boolean isUrl = MapUtils.getAsBoolean(data, "isUrl");
				if (containsDatabaseType && (isUrl == null || !isUrl)) {
					constructURI(data);
				}
				String database_username = MapUtils.getAsString(data, "database_username");
				String plain_password = MapUtils.getAsString(data, "plain_password");
				if (StringUtils.isNotBlank(database_username) && StringUtils.isNotBlank(plain_password)) {
					data.put("database_password", AES256Util.Aes256Encode(plain_password));

					data.remove("plain_password");
				}

				log.info("Handler message start,context: {}", JsonUtil.toJson(context));
				sendMessage(receiver, context);
				log.info("Handler message end,sessionId: {}", context.getSessionId());
			}


		}

	private void constructURI(Map<String, Object> data) {
		if (MapUtils.isEmpty(data)) {
			return;
		}
		String uri = "";

		String database_type = MapUtils.getAsString(data, "database_type");
		if (DataSourceEnum.isMongoDB(database_type)) {
			uri = MONGODB_URI_PREFIX;
			String database_username = MapUtils.getAsString(data, "database_username");
			String database_password = MapUtils.getAsString(data, "database_password");
			if (StringUtils.isNotBlank(database_username) && StringUtils.isNotBlank(database_password)) {
				uri += specialCharHandle(database_username);
				String plain_password;
				try {
					plain_password = AES256Util.Aes256Decode(database_password);
				} catch (Exception e) {
					log.error("Decode failed,message: {}", e.getMessage());
					plain_password = database_password;
				}
				uri += ":" + specialCharHandle(plain_password) + "@";
			}
			uri += MapUtils.getAsString(data, "database_host") + "/";
			uri += MapUtils.getAsString(data, "database_name");
			String additionalString = MapUtils.getAsString(data, "additionalString");
			uri += StringUtils.isNotBlank(additionalString) ? ("?" + additionalString) : "";

			data.put("database_uri", uri);
		}
	}

	private String specialCharHandle(String str) {
		if (StringUtils.isNotBlank(str)){
			try {
				return URLEncoder.encode(str, "UTF-8");
			} catch (Exception e) {
				log.error("Url encode error,message: {}", e.getMessage());
			}
		}
		return str;
	}

	private void sendMessage(String receiver, WebSocketContext context){
		MessageQueueDto messageQueueDto = new MessageQueueDto();
		messageQueueDto.setSender(context.getSender());
		messageQueueDto.setReceiver(receiver);
		messageQueueDto.setData(context.getMessageInfo().getData());
		messageQueueDto.setType(context.getMessageInfo().getType());
		messageQueueService.sendMessage(messageQueueDto);
	}

	private Map<String, Object> testConnectErrorData(String id, String failMessage) {
		Map<String, Object> validate_details = Stream.of(new Object[][]{
				{"status", "failed"},
				{"show_msg", "Connection error"},
				{"fail_message", failMessage},
		}).collect(Collectors.toMap(data -> (String) data[0], data -> data[1]));

		Map<String, Object> response_body = Stream.of(new Object[][]{
				{"validate_details", Collections.singletonList(validate_details)}
		}).collect(Collectors.toMap(data -> (String) data[0], data -> data[1]));

		Map<String, Object> result = Stream.of(new Object[][]{
				{"id", id},
				{"response_body", response_body},
				{"status", "invalid"},
		}).collect(Collectors.toMap(data -> (String) data[0], data -> data[1]));

		return Stream.of(new Object[][]{
				{"type", "testConnectionResult"},
				{"result", result},
				{"status", "SUCCESS"},
		}).collect(Collectors.toMap(data -> (String) data[0], data -> data[1]));
	}
}
