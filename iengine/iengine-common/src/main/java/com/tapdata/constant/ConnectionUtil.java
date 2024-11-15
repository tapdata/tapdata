package com.tapdata.constant;

import com.google.common.collect.Maps;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.flow.engine.V2.entity.GlobalConstant;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Query;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-01-28 10:07
 **/
public class ConnectionUtil {

	private static Logger logger = LogManager.getLogger(ConnectionUtil.class);

	public static void setUniqueName(Connections connections, Object instance) {
		if (connections == null) {
			throw new IllegalArgumentException("Connection cannot be null");
		}
		if (instance == null) {
			connections.setUniqueName(connections.getId());
		}
		Method getUniqueNameMethod = null;
		try {
			getUniqueNameMethod = instance.getClass().getMethod("getUniqueName", Connections.class);
		} catch (NoSuchMethodException e) {
			logger.warn("Not found method: getUniqueName, will use connection id");
			connections.setUniqueName(connections.getId());
		}
		if (getUniqueNameMethod != null) {
			try {
				Object uniqueName = getUniqueNameMethod.invoke(instance, connections);
				if (uniqueName instanceof String) {
					connections.setUniqueName(uniqueName.toString());
				} else {
					connections.setUniqueName(connections.getId());
				}
			} catch (IllegalAccessException e) {
				logger.error("Get connection " + connections.getName() + "'s unique name failed, will use connection id. Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
				connections.setUniqueName(connections.getId());
			} catch (InvocationTargetException e) {
				logger.error("Get connection" + connections.getName() + "'s unique name failed, will use connection id. Error: " +
						e.getTargetException().getMessage() + "\n" + Log4jUtil.getStackString(e.getTargetException()));
				connections.setUniqueName(connections.getId());
			}
		} else {
			connections.setUniqueName(connections.getId());
		}
	}

	public static DatabaseTypeEnum.DatabaseType getDatabaseType(ClientMongoOperator clientMongoOperator, String pdkHash) {
		Map<String, Object> param = Maps.newHashMap();
		param.put("pdkBuildNumber", CommonUtils.getPdkBuildNumer());
		return clientMongoOperator.findOne(
				param,
				ConnectorConstant.DATABASE_TYPE_COLLECTION + "/pdkHash/" + pdkHash,
				DatabaseTypeEnum.DatabaseType.class
		);
	}

	public static DatabaseTypeEnum.DatabaseType getDatabaseType(String pdkHash) {
		ClientMongoOperator clientMongoOperator = ConnectorConstant.clientMongoOperator;
		if (null == clientMongoOperator) {
			throw new RuntimeException("Not found clientMongoOperator");
		}
		return getDatabaseType(clientMongoOperator, pdkHash);
	}

	public static Connections getConnection(String connectionId, List<String> includeFields) {
		ClientMongoOperator clientMongoOperator = ConnectorConstant.clientMongoOperator;
		if (null == clientMongoOperator) {
			throw new RuntimeException("Not found clientMongoOperator");
		}
		Query query = new Query(where("_id").is(connectionId));
		if (CollectionUtils.isNotEmpty(includeFields)) {
			for (String includeField : includeFields) {
				query.fields().include(includeField);
			}
		}
		Connections connections = clientMongoOperator.findOne(
				query,
				ConnectorConstant.CONNECTION_COLLECTION,
				Connections.class
		);
		if (null == connections) {
			throw new RuntimeException("Cannot find connection by id(" + connectionId + ")");
		}
		connections.decodeDatabasePassword();
		connections.initCustomTimeZone();
		return connections;
	}
}
