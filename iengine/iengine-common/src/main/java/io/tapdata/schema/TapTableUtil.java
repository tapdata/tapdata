package io.tapdata.schema;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapTable;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Map;

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
}
