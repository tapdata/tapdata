package io.tapdata.task;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.Schema;
import com.tapdata.entity.Setting;
import com.tapdata.validator.SchemaFactory;
import io.tapdata.TapInterface;
import io.tapdata.common.TapInterfaceUtil;
import io.tapdata.entity.LoadSchemaResult;
import io.tapdata.schema.SchemaProxy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

@TaskType(type = "RELOAD_SCHEMA")
public class ReloadSchemaTask implements Task {

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult result = new TaskResult();
		Map<String, Object> taskData = taskContext.getTaskData();
		try {
			if (taskData.containsKey("connection_id")) {
				String connectionId = (String) taskData.get("connection_id");

				Query query = new Query(where("_id").is(connectionId));
				query.fields().exclude("schema");
				List<Connections> connections = taskContext.getClientMongoOperator().find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);

				if (CollectionUtils.isNotEmpty(connections)) {

					LoadSchemaResult loadSchemaResult = loadSchema(connections.get(0));
					// After load schema, clear schema proxy
					SchemaProxy.getSchemaProxy().clear(connectionId);

					if (StringUtils.isNotBlank(loadSchemaResult.getErrMessage())) {
						result.setTaskResultCode(201);
						result.setTaskResult(loadSchemaResult.getErrMessage());
					} else {
						List<RelateDataBaseTable> relateDataBaseTables = loadSchemaResult.getSchema();

						Update update = new Update();
						update.set("schema.tables", relateDataBaseTables);
						taskContext.getClientMongoOperator().update(query, update, ConnectorConstant.CONNECTION_COLLECTION);

						result.setTaskResultCode(200);
					}

				} else {
					result.setTaskResultCode(201);
					result.setTaskResult("Cannot found connection id " + connectionId + ".");
				}

			} else {
				result.setTaskResultCode(201);
				result.setTaskResult("Must be set the connection id in task_data.");
			}
		} catch (Exception e) {
			String message = e.getMessage();
			result.setTaskResult(message);
			result.setTaskResultCode(201);
		}
		callback.accept(result);
	}

	private LoadSchemaResult loadSchema(Connections connection) throws Exception {

		LoadSchemaResult loadSchemaResult = new LoadSchemaResult();
		setFileDefaultCharset(connection);

		List<RelateDataBaseTable> relateDataBaseTables;

		Schema schema = SchemaFactory.loadSchemaList(connection, true);

		relateDataBaseTables = schema.getTables();

		if (CollectionUtils.isEmpty(relateDataBaseTables)) {
			String databaseType = connection.getDatabase_type();
			TapInterface tapInterface = TapInterfaceUtil.getTapInterface(databaseType, null);
			if (tapInterface != null) {
				loadSchemaResult = tapInterface.loadSchema(connection);
			}
		} else {
			loadSchemaResult.setSchema(relateDataBaseTables);
		}

		return loadSchemaResult;
	}

	private void setFileDefaultCharset(Connections connection) {
		Setting setting = taskContext.getSettingService().getSetting("file.defaultCharset");
		if (setting != null) {
			connection.setFileDefaultCharset(setting.getValue());
		}
	}
}
