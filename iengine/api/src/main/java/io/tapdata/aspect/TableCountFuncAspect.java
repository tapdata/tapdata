package io.tapdata.aspect;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

public class TableCountFuncAspect extends DataFunctionAspect<TableCountFuncAspect> {
	private List<BiConsumer<String, Long>> tableCountConsumerList = new CopyOnWriteArrayList<>();

	public static final int STATE_COUNTING = 10;
	public TableCountFuncAspect tableCountConsumer(BiConsumer<String, Long> consumer) {
		if(!tableCountConsumerList.contains(consumer))
			tableCountConsumerList.add(consumer);
		return this;
	}

	public List<BiConsumer<String, Long>> getTableCountConsumerList() {
		return tableCountConsumerList;
	}

	public void setTableCountConsumerList(List<BiConsumer<String, Long>> tableCountConsumerList) {
		this.tableCountConsumerList = tableCountConsumerList;
	}
}
