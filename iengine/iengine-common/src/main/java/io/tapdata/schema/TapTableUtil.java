package io.tapdata.schema;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-05-29 12:25
 **/
public class TapTableUtil {
	public static Map<String, String> getTableNameQualifiedNameMap(String nodeId) {
		return BeanUtil.getBean(ClientMongoOperator.class)
				.findOne(Query.query(where("nodeId").is(nodeId)),
						ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/node/tableMap", Map.class);
	}

	public static String getHeartbeatQualifiedName(String nodeId) {
		return BeanUtil.getBean(ClientMongoOperator.class)
				.findOne(Query.query(where("nodeId").is(nodeId)),
						ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/node/heartbeatQualifiedName", String.class);
	}

	public static TapTableMap<String, TapTable> getTapTableMapByNodeId(String nodeId) {
		Map<String, String> tableNameQualifiedNameMap = getTableNameQualifiedNameMap(nodeId);
		return TapTableMap.create(nodeId, tableNameQualifiedNameMap);
	}

	public static TapTableMap<String, TapTable> getTapTableMapByNodeId(String nodeId, Long time) {
		return getTapTableMapByNodeId(null, nodeId, time);
	}

	public static TapTableMap<String, TapTable> getTapTableMapByNodeId(String prefix, String nodeId, Long time) {
		Map<String, String> tableNameQualifiedNameMap = getTableNameQualifiedNameMap(nodeId);
		return TapTableMap.create(prefix, nodeId, tableNameQualifiedNameMap, time);
	}

	@NotNull
	public static TapTableMap<String, TapTable> getTapTableMap(Node<?> node, Long tmCurrentTime) {
		return getTapTableMap(null, node, tmCurrentTime);
	}

	/**
	 * 获取节点的模型
	 * @param node
	 * @param tmCurrentTime
	 * @return
	 */
	@NotNull
	public static TapTableMap<String, TapTable> getTapTableMap(String prefix, Node<?> node, Long tmCurrentTime) {

		Object schema = node.getSchema();
		if (schema == null) {
			List inputSchema = node.getInputSchema();
			schema = node.mergeSchema(inputSchema, null);
		}
		List<Schema> schemaList = null;
		if (schema != null) {
			if (schema instanceof Schema) {
				schemaList = Collections.singletonList((Schema) schema);
			} else if (schema instanceof List) {
				schemaList = (List<Schema>) schema;
			}
		}
		TapTableMap<String, TapTable> tapTableMap;
		if (CollectionUtils.isNotEmpty(schemaList)) {
			List<TapTable> tapTableList = schemaList.stream().map(PdkSchemaConvert::toPdk).collect(Collectors.toList());
			tapTableMap = TapTableMap.create(prefix, node.getId(), tapTableList, tmCurrentTime);
		} else {
			tapTableMap = TapTableMap.create(node.getId());
		}
		return tapTableMap;
	}
}
