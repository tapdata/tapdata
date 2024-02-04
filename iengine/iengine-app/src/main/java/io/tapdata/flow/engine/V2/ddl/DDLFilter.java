package io.tapdata.flow.engine.V2.ddl;

import com.tapdata.tm.commons.dag.DDLConfiguration;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;

import java.util.List;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-07-14 11:07
 **/
public class DDLFilter implements Predicate<TapDDLEvent> {

	private List<String> disabledEvents;
	private Predicate<String> dynamicTableTest;

	private DDLConfiguration configuration;

	private DDLFilter() {
	}

	public static DDLFilter create(List<String> disabledEvents,DDLConfiguration ddlConfiguration) {
		return new DDLFilter()
				.disabledEvents(disabledEvents)
				.ddlConfiguration(ddlConfiguration);
	}

	public DDLFilter disabledEvents(List<String> disabledEvents) {
		this.disabledEvents = disabledEvents;
		return this;
	}

	public DDLFilter dynamicTableTest(Predicate<String> dynamicTableTest) {
		this.dynamicTableTest = dynamicTableTest;
		return this;
	}

	public DDLFilter ddlConfiguration(DDLConfiguration ddlConfiguration) {
		this.configuration = ddlConfiguration;
		return this;
	}

	@Override
	public boolean test(TapDDLEvent tapDDLEvent) {
		if (null != dynamicTableTest && dynamicTableTest.test(tapDDLEvent.getTableId())) {
			if (tapDDLEvent instanceof TapCreateTableEvent || tapDDLEvent instanceof TapDropTableEvent) {
				return true;
			}
		}
		if(null != configuration){
			switch (configuration){
				case ERROR:
					throw new TapCodeException(TaskProcessorExCode_11.ENCOUNTERED_DDL_EVENT_REPORT_ERROR);
				case FILTER:
					return false;
				case SYNCHRONIZATION:
					if(tapDDLEvent instanceof TapDDLUnknownEvent){
						throw new TapCodeException(TaskProcessorExCode_11.UNABLE_TO_SYNCHRONIZE_DDL_EVENT);
					}
					String key = tapDDLEvent.key();
					if (null != disabledEvents && !disabledEvents.contains(key)) {
						return true;
					}
					break;
				default:
					return false;
			}
		}
		return false;
	}
}
