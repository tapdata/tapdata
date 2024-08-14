package io.tapdata.flow.engine.V2.ddl;

import com.tapdata.tm.commons.dag.DDLConfiguration;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.flow.engine.V2.util.PartitionTableUtil;
import io.tapdata.observable.logging.ObsLogger;

import java.util.List;

public class PartitionDDLFilter extends DDLFilter {

    public static DDLFilter instance(List<String> disabledEvents, DDLConfiguration ddlConfiguration, String ignoreDDLRules, ObsLogger obsLogger) {
        return new PartitionDDLFilter()
                .disabledEvents(disabledEvents)
                .ddlConfiguration(ddlConfiguration)
                .ignoreDDLRules(ignoreDDLRules)
                .obsLogger(obsLogger);
    }

    @Override
    public boolean checkDDLFirst(TapDDLEvent tapDDLEvent) {
        if (tapDDLEvent instanceof TapCreateTableEvent) {
            if (PartitionTableUtil.checkIsSubPartitionTable(((TapCreateTableEvent)tapDDLEvent).getTable())) {
                return true;
            }
        }
        if (tapDDLEvent instanceof TapDropTableEvent) {
            return true;
        }
        return false;
    }
}
