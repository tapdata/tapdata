package io.tapdata.aspect;

import io.tapdata.entity.CountResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

public class TableCountFuncAspect extends DataFunctionAspect<TableCountFuncAspect> {
	private List<BiConsumer<String, CountResult>> tableCountConsumerList = new CopyOnWriteArrayList<>();

	public static final int STATE_COUNTING = 10;
	public TableCountFuncAspect tableCountConsumer(BiConsumer<String, CountResult> consumer) {
		if(!tableCountConsumerList.contains(consumer))
			tableCountConsumerList.add(consumer);
		return this;
	}

	public List<BiConsumer<String, CountResult>> getTableCountConsumerList() {
		return tableCountConsumerList;
	}

	public void setTableCountConsumerList(List<BiConsumer<String, CountResult>> tableCountConsumerList) {
		this.tableCountConsumerList = tableCountConsumerList;
	}
}
