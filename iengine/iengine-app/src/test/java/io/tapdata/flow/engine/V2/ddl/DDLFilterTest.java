package io.tapdata.flow.engine.V2.ddl;

import com.tapdata.tm.commons.dag.DDLConfiguration;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class DDLFilterTest {
    @Test
    @DisplayName("Throws an exception when encountering a DDL event")
    void testDDL_Error(){
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.ERROR,null,null);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        TapCodeException tapCodeException = Assertions.assertThrows(TapCodeException.class, () -> ddlFilter.test(tapAlterFieldNameEvent));
        assertEquals(TaskProcessorExCode_11.ENCOUNTERED_DDL_EVENT_REPORT_ERROR,tapCodeException.getCode());
    }

    @Test
    @DisplayName("Regularly match DDL events and filter")
    void testDDL_Error_MatchFilter(){
        String rulers = "ALTER\\s+TABLE\\s+\"([^\"]+)\"\\.\"([^\"]+)\"\\s+ADD\\s+\\(\"([^\"]+)\"\\s+[^\\)]+\\)";
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.ERROR,rulers,mock(ObsLogger.class));
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        tapAlterFieldNameEvent.setOriginDDL("ALTER TABLE \"C##TAPDATA\".\"TT_DDL\" \n" +
                "ADD (\"TT\" VARCHAR2(255));");
        Assertions.assertFalse(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Filter DDL events")
    void testDDL_Filter(){
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.FILTER,null,null);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertFalse(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Synchronization DDL events")
    void testDDL_Synchronization(){
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.SYNCHRONIZATION,null,null);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertTrue(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Synchronization DDL events")
    void testDDL_Synchronization_disabledEvents(){
        List<String> disabledEvents = new ArrayList<>();
        disabledEvents.add("alter_field_name_event");
        DDLFilter ddlFilter = DDLFilter.create(disabledEvents, DDLConfiguration.SYNCHRONIZATION,null,null);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertFalse(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Synchronization DDL events")
    void testDDL_Synchronization_unknownEvent(){
        List<String> disabledEvents = new ArrayList<>();
        disabledEvents.add("alter_field_name_event");
        DDLFilter ddlFilter = DDLFilter.create(disabledEvents, DDLConfiguration.SYNCHRONIZATION,null,null);
        TapDDLUnknownEvent tapDDLUnknownEvent = new TapDDLUnknownEvent();
        TapCodeException tapCodeException = Assertions.assertThrows(TapCodeException.class, () -> ddlFilter.test(tapDDLUnknownEvent));
        assertEquals(tapCodeException.getCode(),TaskProcessorExCode_11.UNABLE_TO_SYNCHRONIZE_DDL_EVENT);
    }

    @Test
    void testDynamicTableTest() {
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), null, null, null)
                .dynamicTableTest(tapDDLEvent -> tapDDLEvent instanceof TapDropTableEvent);

        TapDropTableEvent event = new TapDropTableEvent();
        boolean result = ddlFilter.test(event);
        Assertions.assertTrue(result);

        result = ddlFilter.test(new TapCreateTableEvent());
        Assertions.assertFalse(result);
    }
}
