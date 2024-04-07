package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.Runnable.KafkaLoadSchemaRunner;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.service.skeleton.annotation.RemoteService;
import java.util.List;
import java.util.Map;

@RemoteService
public class DiscoverSchemaService {
	public void discoverSchema(String connectionId, Map<String, Object> nodeConfig) {
		PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
		if (pdkUtils == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "pdkUtils is null");

		if (connectionId == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "connectionId is null");
		Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
		if (connections == null)
			throw new CoreException(NetErrors.CONNECTIONS_NOT_FOUND, "Connections {} not found", connectionId);

		PDKUtils.PDKInfo pdkInfo = pdkUtils.downloadPdkFileIfNeed(connections.getPdkHash());

		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		LoadSchemaRunner loadSchemaRunner = new LoadSchemaRunner(connections, clientMongoOperator, 0, nodeConfig);
		loadSchemaRunner.run();
	}
	public Object discoverSchemaMQ(String connectionId, String taskId, Map<String, Object> nodeConfig, List<String> tableName){
		Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
		if (connections == null)
			throw new CoreException(NetErrors.CONNECTIONS_NOT_FOUND, "Connections {} not found", connectionId);
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		KafkaLoadSchemaRunner kafkaLoadSchemaRunner = new KafkaLoadSchemaRunner(nodeConfig, connections,clientMongoOperator,tableName);
		Map<String, Object> tapTableMap = kafkaLoadSchemaRunner.run();
		return tapTableMap ;
	}
}
