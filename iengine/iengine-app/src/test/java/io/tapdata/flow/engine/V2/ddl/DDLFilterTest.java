package io.tapdata.flow.engine.V2.ddl;

import com.tapdata.tm.commons.dag.DDLConfiguration;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DDLFilterTest {
    @Test
    @DisplayName("Throws an exception when encountering a DDL event")
    void testDDL_Error(){
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.ERROR);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertThrows(TapCodeException.class,()->ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Filter DDL events")
    void testDDL_Filter(){
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.FILTER);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertFalse(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Synchronization DDL events")
    void testDDL_Synchronization(){
        DDLFilter ddlFilter = DDLFilter.create(new ArrayList<>(), DDLConfiguration.SYNCHRONIZATION);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertTrue(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Synchronization DDL events")
    void testDDL_Synchronization_disabledEvents(){
        List<String> disabledEvents = new ArrayList<>();
        disabledEvents.add("alter_field_name_event");
        DDLFilter ddlFilter = DDLFilter.create(disabledEvents, DDLConfiguration.SYNCHRONIZATION);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        Assertions.assertFalse(ddlFilter.test(tapAlterFieldNameEvent));
    }

    @Test
    @DisplayName("Synchronization DDL events")
    void testDDL_Synchronization_unknownEvent(){
        List<String> disabledEvents = new ArrayList<>();
        disabledEvents.add("alter_field_name_event");
        DDLFilter ddlFilter = DDLFilter.create(disabledEvents, DDLConfiguration.SYNCHRONIZATION);
        TapDDLUnknownEvent tapDDLUnknownEvent = new TapDDLUnknownEvent();
        Assertions.assertThrows(TapCodeException.class,()->ddlFilter.test(tapDDLUnknownEvent));
    }
}
