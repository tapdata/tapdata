package partition;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class CountByFilterHandler implements CountByPartitionFilterFunction {
	private List<Map<String, Object>> records;
	public CountByFilterHandler(List<Map<String, Object>> records) {
		this.records = records;
	}
	@Override
	public long countByPartitionFilter(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter filter) {
		return 0;
	}
}
