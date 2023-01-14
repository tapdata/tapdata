package io.tapdata.flow.engine.V2.ddl;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;

import java.util.List;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-07-14 11:07
 **/
public class DDLFilter implements Predicate<TapDDLEvent> {

	private Boolean enableDDL;
	private List<String> disabledEvents;
	private Predicate<String> dynamicTableTest;

	private DDLFilter() {
	}

	public static DDLFilter create(Boolean enableDDL, List<String> disabledEvents) {
		return new DDLFilter()
				.enableDDL(enableDDL)
				.disabledEvents(disabledEvents);
	}

	public DDLFilter enableDDL(Boolean enableDDL) {
		this.enableDDL = enableDDL;
		return this;
	}

	public DDLFilter disabledEvents(List<String> disabledEvents) {
		this.disabledEvents = disabledEvents;
		return this;
	}

	public DDLFilter dynamicTableTest(Predicate<String> dynamicTableTest) {
		this.dynamicTableTest = dynamicTableTest;
		return this;
	}

	@Override
	public boolean test(TapDDLEvent tapDDLEvent) {
		if (null != enableDDL && enableDDL) {
			String key = tapDDLEvent.key();
			if (null != disabledEvents && !disabledEvents.contains(key)) {
				return true;
			}
		}
		if (null != dynamicTableTest && dynamicTableTest.test(tapDDLEvent.getTableId())) {
			if (tapDDLEvent instanceof TapCreateTableEvent || tapDDLEvent instanceof TapDropTableEvent) {
				return true;
			}
		}
		return false;
	}
}
