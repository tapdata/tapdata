package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Iterator;
import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-06-14 16:45
 **/
public class HazelcastSourcePdkShareCDCNode extends HazelcastSourcePdkDataNode {

	private Logger logger = LogManager.getLogger(HazelcastSourcePdkShareCDCNode.class);

	public HazelcastSourcePdkShareCDCNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.sourceMode = SourceMode.LOG_COLLECTOR;
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		Node<?> node = dataProcessorContext.getNode();
		if (!(node instanceof LogCollectorNode)) {
			throw new RuntimeException("Expected LogCollectorNode, actual is: " + node.getClass().getName());
		}
		List<String> connectionIds = ((LogCollectorNode) node).getConnectionIds();
		Iterator<String> iterator = connectionIds.iterator();
		String connectionId = iterator.next();
		Query connectionQuery = new Query(where("_id").is(connectionId));
		connectionQuery.fields().include("config").include("pdkHash");
		Connections srcConnection = clientMongoOperator.findOne(connectionQuery, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, srcConnection.getPdkHash());
		dataProcessorContext = DataProcessorContext.newBuilder()
				.withTaskDto(dataProcessorContext.getTaskDto())
				.withNode(dataProcessorContext.getNode())
				.withNodes(dataProcessorContext.getNodes())
				.withEdges(dataProcessorContext.getEdges())
				.withConfigurationCenter(dataProcessorContext.getConfigurationCenter())
				.withConnectionConfig(srcConnection.getConfig())
				.withDatabaseType(databaseType)
				.withTapTableMap(dataProcessorContext.getTapTableMap())
				.withTaskConfig(dataProcessorContext.getTaskConfig())
				.withConnections(dataProcessorContext.getConnections())
				.build();
		this.syncType = SyncTypeEnum.CDC;
		super.doInit(context);
	}
}
