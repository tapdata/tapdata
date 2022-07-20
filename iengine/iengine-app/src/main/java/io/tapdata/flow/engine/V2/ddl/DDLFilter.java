package io.tapdata.flow.engine.V2.ddl;

import io.tapdata.entity.event.ddl.TapDDLEvent;

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

	@Override
	public boolean test(TapDDLEvent tapDDLEvent) {
		if (null == enableDDL) {
			return false;
		}
		if (!enableDDL) {
			return false;
		}
		if (null == disabledEvents) {
			return false;
		}
		String key = tapDDLEvent.key();
		if (disabledEvents.contains(key)) {
			return false;
		}
		return true;
	}
}
