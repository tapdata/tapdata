package io.tapdata.aspect;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class TableCountFuncAspect extends DataFunctionAspect<TableCountFuncAspect> {
	private BiConsumer<String, Long> tableCountConsumer;
	public TableCountFuncAspect tableCountConsumer(BiConsumer<String, Long> consumer) {
		tableCountConsumer = consumer;
		return this;
	}

	public BiConsumer<String, Long> getTableCountConsumer() {
		return tableCountConsumer;
	}

	public void setTableCountConsumer(BiConsumer<String, Long> tableCountConsumer) {
		this.tableCountConsumer = tableCountConsumer;
	}
}
