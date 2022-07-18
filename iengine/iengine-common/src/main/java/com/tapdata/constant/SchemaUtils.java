package com.tapdata.constant;

import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author samuel
 * @Description
 * @create 2020-09-21 15:30
 **/
public class SchemaUtils {
	public static RelateDatabaseField findAutoIncrementField(RelateDataBaseTable relateDataBaseTable) {
		if (relateDataBaseTable == null || CollectionUtils.isEmpty(relateDataBaseTable.getFields())) {
			return null;
		}

		RelateDatabaseField autoIncrementField = relateDataBaseTable.getFields().stream()
				.filter(relateDatabaseField -> relateDatabaseField.getAutoincrement().equalsIgnoreCase("yes"))
				.findFirst().orElse(null);

		return autoIncrementField;
	}
}
