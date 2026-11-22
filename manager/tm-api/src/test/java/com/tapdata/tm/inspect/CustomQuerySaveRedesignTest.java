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

    @Test void partialPatchWithoutTasksIsAllowedButCreateAndEmptyReplacementAreRejected() {
        InspectDto patch = new InspectDto(); patch.setId(new org.bson.types.ObjectId()); patch.setInspectMethod("custom_query");
        CustomQueryInspectSupport support = new CustomQueryInspectSupport();
        assertDoesNotThrow(() -> support.validate(patch,null));
        patch.setTasks(Collections.emptyList()); assertThrows(IllegalArgumentException.class,()->support.validate(patch,null));
        patch.setId(null); patch.setTasks(null); assertThrows(IllegalArgumentException.class,()->support.validate(patch,null));
    }

    @Test void savingUsesSnapshotTimezoneWithoutRequiringClientDuplication() {
        InspectDto inspect = new InspectDto(); inspect.setInspectMethod("custom_query");
        Task task = new Task(); CustomQueryConfig config = new CustomQueryConfig();
        CustomQueryConfig.Mapping mapping = new CustomQueryConfig.Mapping(); mapping.setSource("v");mapping.setTarget("v");
        config.setMappings(List.of(mapping)); task.setCustomQueryConfig(config);
        Source side = new Source();side.setConnectionId("c");side.setCustomQuery("select 1 v");
        CustomQueryConfig.Snapshot snapshot = new CustomQueryConfig.Snapshot();snapshot.setTimezone("Asia/Shanghai");
        CustomQueryConfig.Column column = new CustomQueryConfig.Column();column.setName("v");snapshot.setColumns(List.of(column));
        CustomQueryRequest tested = new CustomQueryRequest();tested.setConnectionId("c");tested.setQuery("select 1 v");tested.setTimezone(snapshot.getTimezone());
        snapshot.setFingerprint(tested.fingerprint());side.setQuerySnapshot(snapshot);task.setSource(side);task.setTarget(side);inspect.setTasks(List.of(task));
        List<String> authorizedZones = new ArrayList<>();
        CustomQueryInspectSupport support = new CustomQueryInspectSupport() {
            @Override public void authorize(CustomQueryRequest request, com.tapdata.tm.config.security.UserDetail user) {
                request.validate();authorizedZones.add(request.getTimezone());
            }
        };
        assertDoesNotThrow(()->support.validate(inspect,null));
        assertEquals(List.of("Asia/Shanghai","Asia/Shanghai"),authorizedZones);
        side.setQueryTimezone("UTC"); assertThrows(IllegalArgumentException.class,()->support.validate(inspect,null));
    }

}
