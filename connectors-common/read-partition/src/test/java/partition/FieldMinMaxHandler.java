package partition;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.QueryFieldMinMaxValueFunction;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public abstract class FieldMinMaxHandler implements QueryFieldMinMaxValueFunction, CountByPartitionFilterFunction {
	private List<Map<String, Object>> records;
	public FieldMinMaxHandler(List<Map<String, Object>> records) {
		this.records = records;
	}

	@Override
	public FieldMinMaxValue minMaxValue(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter advanceFilter, String fieldName) {
		TapPartitionFilter filter = new TapPartitionFilter().fromAdvanceFilter(advanceFilter);
		QueryOperator left = filter.getLeftBoundary();
		QueryOperator right = filter.getRightBoundary();
		DataMap matchMap = filter.getMatch();
		List<Map<String, Object>> newRecords = new ArrayList<>();

		Object min = null;
		Object max = null;
		for(Map<String, Object> record : records) {
			if(matchMap != null) {
				boolean matched = true;
				for (Map.Entry<String, Object> entry : matchMap.entrySet()) {
					if(!entry.getValue().equals(record.get(entry.getKey()))) {
						matched = false;
						break;
					}
				}
				if(!matched) {
					continue;
				}
			}
			if(left != null && !match(left, record)) {
				continue;
			}
			if(right != null && !match(right, record)) {
				continue;
			}
			if(min == null || largerThan(min, record.get(fieldName)))
				min = record.get(fieldName);

			if(max == null || largerThan(record.get(fieldName), max))
				max = record.get(fieldName);

			newRecords.add(record);
		}
		if(min != null)
			return new FieldMinMaxValue().min(min).max(max).detectType(min).fieldName(fieldName);
		return null;
	}
	@Override
	public long countByPartitionFilter(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter advanceFilter) {
		TapPartitionFilter filter = TapPartitionFilter.create().fromAdvanceFilter(advanceFilter);
		QueryOperator left = filter.getLeftBoundary();
		QueryOperator right = filter.getRightBoundary();
		DataMap matchMap = filter.getMatch();
		List<Map<String, Object>> newRecords = new ArrayList<>();

		Object min = null;
		Object max = null;
		for(Map<String, Object> record : records) {
			if(matchMap != null) {
				boolean matched = true;
				for (Map.Entry<String, Object> entry : matchMap.entrySet()) {
					if(!entry.getValue().equals(record.get(entry.getKey()))) {
						matched = false;
						break;
					}
				}
				if(!matched) {
					continue;
				}
			}
			if(left != null && !match(left, record)) {
				continue;
			}
			if(right != null && !match(right, record)) {
				continue;
			}
			newRecords.add(record);
		}
		return newRecords.size();
	}

	protected abstract boolean match(QueryOperator right, Map<String, Object> record);

	protected abstract boolean largerThan(Object a, Object b);
}
