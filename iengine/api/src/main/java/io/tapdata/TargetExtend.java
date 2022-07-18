package io.tapdata;

import com.tapdata.entity.*;
import io.tapdata.exception.TargetException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Target extend api
 *
 * @author samuel
 */
public interface TargetExtend extends BaseExtend {

	/**
	 * Clear target table data before sync
	 *
	 * @throws TargetException
	 */
	default void deleteTargetTables() throws TargetException {
	}

	/**
	 * Clear target table schema before sync
	 *
	 * @throws TargetException
	 */
	default void deleteTargetSchema(Connections targertConn, String name, String type) throws TargetException {
	}

	/**
	 * Is need to create index
	 *
	 * @param tableName
	 * @param condition
	 * @param targetConn
	 */
	default boolean needCreateIndex(String tableName, List<Map<String, String>> condition, Connections targetConn) {
		if (StringUtils.isBlank(tableName) || CollectionUtils.isEmpty(condition) || targetConn == null) {
			return false;
		}

		if (StringUtils.equalsAnyIgnoreCase(targetConn.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
			// mongodb target _id no need to create index
			Set<String> keys = new LinkedHashSet<>();
			condition.forEach(map -> map.keySet().forEach(key -> keys.add(key)));
			if (keys.size() == 1 && "_id".equals(keys.iterator().next())) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Auto create index on target table
	 * Based on join unique key(s) or array unique key(s)
	 *
	 * @param tableName
	 * @param condition
	 * @throws TargetException
	 */
	default void createIndex(String tableName, List<Map<String, String>> condition) throws TargetException {
	}

	/**
	 * Is support to copy source indices
	 */
	default boolean supportCopySourceIndex() {
		return false;
	}

	/**
	 * Copy source indices
	 *
	 * @param tableName
	 * @param tableIndex
	 * @param fieldProcessByFieldName
	 * @throws TargetException
	 */
	default void copySourceIndex(String tableName, TableIndex tableIndex, Map<String, FieldProcess> fieldProcessByFieldName) throws TargetException {
	}

	/**
	 * Is support to create target table
	 */
	default boolean supportCreateTargetTable() {
		return false;
	}

	/**
	 * Create target table by target schema, which calculate from source schema when job running
	 *
	 * @throws SQLException
	 */
	default void createTargetTableBySchema(RelateDataBaseTable relateDataBaseTable) throws Exception {
	}

	/**
	 * Will run this method after io.tapdata.TargetExtend#createTargetTableBySchema(com.tapdata.entity.RelateDataBaseTable)
	 * Do some thing depends on io.tapdata.TargetExtend#createTargetTableBySchema(com.tapdata.entity.RelateDataBaseTable)
	 *
	 * @throws Exception
	 */
	default void afterCreateTargetTable() throws Exception {
	}
}
