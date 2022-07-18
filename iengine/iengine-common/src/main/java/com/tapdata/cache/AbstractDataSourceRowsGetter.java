package com.tapdata.cache;

import java.util.List;
import java.util.Map;

public abstract class AbstractDataSourceRowsGetter implements IDataSourceRowsGetter {
	private final Map<String, Map<String, Integer>> stageFieldProjectionMap;

	public AbstractDataSourceRowsGetter(Map<String, Map<String, Integer>> stageFieldProjectionMap) {
		this.stageFieldProjectionMap = stageFieldProjectionMap;
	}

	@Override
	public List<Map<String, Object>> getRows(Object[] keys) {
		List<Map<String, Object>> rows = getRowsFromDatabase();
//    for (Map<String, Object> row : rows) {
//      Map<String, Map<String, Integer>> stageFieldProjection = getStageFieldProjection();
//      if (MapUtils.isNotEmpty(stageFieldProjection) && stageFieldProjection.containsKey(sourceStageId)) {
//        Map<String, Integer> fieldProjection = stageFieldProjection.get(sourceStageId);
//        DataFlowStageUtil.fieldProjection(row, fieldProjection);
//      }
//    }
		return rows;
	}

	abstract protected List<Map<String, Object>> getRowsFromDatabase();

	protected Map<String, Map<String, Integer>> getStageFieldProjection() {
		return stageFieldProjectionMap;
	}
}
