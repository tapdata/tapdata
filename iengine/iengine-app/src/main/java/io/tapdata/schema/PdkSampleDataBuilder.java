package io.tapdata.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PdkSampleDataBuilder implements SampleDataBuilder {
	private final TapTable tapTable;
	private final QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
	private final TapConnectorContext connectorContext;

	public PdkSampleDataBuilder(TapTable tapTable, QueryByAdvanceFilterFunction queryByAdvanceFilterFunction, TapConnectorContext connectorContext) {
		this.tapTable = tapTable;
		this.queryByAdvanceFilterFunction = queryByAdvanceFilterFunction;
		this.connectorContext = connectorContext;
	}

	@Override
	public List<Map<String, Object>> get(int limit) throws Throwable {
		TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(limit);
		AtomicReference<List<Map<String, Object>>> resultList = new AtomicReference<>();
		AtomicReference<Throwable> throwable = new AtomicReference<>();
		queryByAdvanceFilterFunction.query(connectorContext, tapAdvanceFilter, tapTable, filterResults -> {
			Throwable error = filterResults.getError();
			if (null != error) {
				throwable.set(error);
			}
			resultList.set(filterResults.getResults());
		});

		if (null != throwable.get()) {
			throw new RuntimeException(throwable.get());
		}

		return resultList.get();
	}
}
