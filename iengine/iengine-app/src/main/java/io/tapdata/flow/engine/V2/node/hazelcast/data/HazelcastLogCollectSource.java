package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.schema.SchemaList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-02-07 14:40
 **/
public class HazelcastLogCollectSource extends HazelcastTaskSource {

	private Logger logger = LogManager.getLogger(HazelcastLogCollectSource.class);

	public HazelcastLogCollectSource(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Processor.Context context) throws TapCodeException {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		Node<?> node = dataProcessorContext.getNode();
		if (!(node instanceof LogCollectorNode)) {
			throw new RuntimeException("Expected LogCollectorNode, actual is: " + node.getClass().getName());
		}
		List<String> connectionIds = ((LogCollectorNode) node).getConnectionIds();
		Iterator<String> iterator = connectionIds.iterator();
		while (iterator.hasNext()) {
			String connectionId = iterator.next();
			dataProcessorContext = DataProcessorContext.newBuilder()
					.withTaskDto(taskDto)
					.withNode(dataProcessorContext.getNode())
					.withNodes(dataProcessorContext.getNodes())
					.withEdges(dataProcessorContext.getEdges())
					.withSourceConn(MongodbUtil.getConnections(new Query(where("_id").is(connectionId)), clientMongoOperator, true))
					.withConfigurationCenter(dataProcessorContext.getConfigurationCenter())
					.build();
			List<String> tableNames = ((LogCollectorNode) node).getTableNames();
			String selectType = ((LogCollectorNode) node).getSelectType();
			this.mappings = new ArrayList<>();
			Map<String, List<RelateDataBaseTable>> schema = dataProcessorContext.getSourceConn().getSchema();
			switch (selectType) {
				case LogCollectorNode.SELECT_TYPE_ALL:
					// 所有表
					tableNames.addAll(((SchemaList<String, RelateDataBaseTable>) schema).getTableNames());
					break;
				case LogCollectorNode.SELECT_TYPE_RESERVATION:
					// 保留表
					break;
				case LogCollectorNode.SELECT_TYPE_EXCLUSIONTABLE:
					// 排除表
					List<String> finalTableNames = tableNames;
					tableNames = ((SchemaList<String, RelateDataBaseTable>) schema).getTableNames().stream()
							.filter(tableName -> !finalTableNames.contains(tableName)).collect(Collectors.toList());
					break;
			}
			tableNames.forEach(tableName -> {
				Mapping mapping = new Mapping();
				mapping.setFrom_table(tableName);
				this.mappings.add(mapping);
			});
			this.syncType = SyncTypeEnum.CDC;
			try {
				super.doInit(context);
				break;
			} catch (Exception e) {
				if (iterator.hasNext()) {
					logger.warn("Running log collect source with connection: " + dataProcessorContext.getSourceConn().getName() +
							" failed. Will try next connection" + "\n" + Log4jUtil.getStackString(e));
				} else {
					logger.error("Running log collect source with connection: " + dataProcessorContext.getSourceConn().getName() +
							" failed. Will try next connection" + "\n" + Log4jUtil.getStackString(e));
					break;
				}
			}
		}
	}
}
