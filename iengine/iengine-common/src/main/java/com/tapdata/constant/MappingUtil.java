package com.tapdata.constant;

import com.tapdata.entity.Mapping;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MappingUtil {

	public static Map<String, List<Mapping>> adaptMappingsToTableMappings(List<Mapping> mappings) {
		Map<String, List<Mapping>> tableMappings = new HashMap<>();
		if (mappings != null) {
			for (Mapping mapping : mappings) {
				String fromTable = mapping.getFrom_table();
				if (tableMappings.containsKey(fromTable)) {
					tableMappings.get(fromTable).add(mapping);
				} else {
					List<Mapping> tableMap = new ArrayList<>();
					tableMap.add(mapping);
					tableMappings.put(fromTable, tableMap);
				}
			}
		}
		return tableMappings;
	}

	public static List<String> getTablePrimaryKeys(Mapping mapping) {
		List<String> pks = null;
		String relationship = mapping.getRelationship();
		List<Map<String, String>> conditions = null;
		switch (relationship) {
			case ConnectorConstant.RELATIONSHIP_ONE_ONE:
			case ConnectorConstant.RELATIONSHIP_ONE_MANY:
				conditions = mapping.getJoin_condition();
				break;
			case ConnectorConstant.RELATIONSHIP_MANY_ONE:
				conditions = mapping.getMatch_condition();
				break;
			default:
				break;
		}

		if (CollectionUtils.isNotEmpty(conditions)) {
			pks = new ArrayList<>();
			for (Map<String, String> condition : conditions) {
				pks.add(condition.get("source"));
			}
		}

		return pks;
	}
}
