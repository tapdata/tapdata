package com.tapdata.tm.inspect;

import com.tapdata.tm.commons.inspect.*;
import com.tapdata.tm.inspect.bean.*;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.CustomQueryInspectSupport;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CustomQuerySaveRedesignTest {
    @Test void saveRejectsUnsafeSqlBeforeCallingConnectionsOrPersistence() {
        InspectDto inspect=new InspectDto();inspect.setInspectMethod("custom_query");Task task=new Task();
        CustomQueryConfig config=new CustomQueryConfig();CustomQueryConfig.Mapping mapping=new CustomQueryConfig.Mapping();mapping.setSource("v");mapping.setTarget("v");config.setMappings(List.of(mapping));task.setCustomQueryConfig(config);
        Source source=new Source();source.setConnectionId("000000000000000000000001");source.setCustomQuery("DELETE FROM customer");
        CustomQueryRequest request=new CustomQueryRequest();request.setConnectionId(source.getConnectionId());request.setQuery(source.getCustomQuery());request.setTimezone(source.getQueryTimezone());
        CustomQueryConfig.Snapshot snapshot=new CustomQueryConfig.Snapshot();snapshot.setFingerprint(request.fingerprint());CustomQueryConfig.Column column=new CustomQueryConfig.Column();column.setName("v");snapshot.setColumns(List.of(column));source.setQuerySnapshot(snapshot);task.setSource(source);task.setTarget(source);inspect.setTasks(List.of(task));
        assertThrows(IllegalArgumentException.class,()->new CustomQueryInspectSupport().validate(inspect,null));
    }
    @Test void oldTaskMustRetestInsteadOfInferringKeysFromSortFields() {
        InspectDto inspect=new InspectDto();inspect.setInspectMethod("custom_query");Task task=new Task();task.setSortFields(List.of("id"));inspect.setTasks(List.of(task));
        assertThrows(IllegalArgumentException.class,()->new CustomQueryInspectSupport().validate(inspect,null));
    }
}
