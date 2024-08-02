package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.file.service.FileService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/2 18:42
 */
@ExtendWith(MockitoExtension.class)
public class DatabaseTypeScheduleTest {

    @Mock
    private DataSourceDefinitionService dataSourceDefinitionService;
    @Mock
    private FileService fileService;

    @Test
    public void testCleanUpForDatabaseTypes() {

        assertNotNull(dataSourceDefinitionService);
        assertNotNull(fileService);

        List<ObjectId> originalIds = Arrays.asList(
                new ObjectId("66ace100fb85b005263f2c13"),
                new ObjectId("66ace100fb85b005263f2c14"),
                new ObjectId("66ace100fb85b005263f2c15"),
                new ObjectId("66ace100fb85b005263f2c16"),
                new ObjectId("66ace100fb85b005263f2c17")
        );

        DatabaseTypeSchedule databaseTypeSchedule = new DatabaseTypeSchedule();
        databaseTypeSchedule.setFileService(fileService);
        databaseTypeSchedule.setDataSourceDefinitionService(dataSourceDefinitionService);

        Query query = Query.query(Criteria.where("is_deleted").is(true));
        query.fields().include("id", "is_deleted",
                "jarRid", "icon", "messages.zh_CN.doc", "messages.zh_TW.doc", "messages.en_US.doc");
        List<DataSourceDefinitionDto> result = new ArrayList<>();
        DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
        dto.setId(new ObjectId());
        dto.setJarRid(originalIds.get(0).toHexString());
        dto.setIcon(originalIds.get(1).toHexString());

        LinkedHashMap<String, Object> messages = new LinkedHashMap<>();
        dto.setMessages(messages);
        messages.put("zh_CN", new HashMap<String, Object>(){{
            put("doc", originalIds.get(2));
        }});
        messages.put("zh_TW", new HashMap<String, Object>(){{
            put("doc", originalIds.get(3).toHexString());
        }});
        messages.put("en_US", new HashMap<String, Object>(){{
            put("doc", originalIds.get(4));
        }});
        messages.put("en_DE", new HashMap<String, Object>());
        messages.put("en_UE", new HashMap<String, Object>(){{
            put("doc", true);
        }});
        messages.put("test", new ArrayList<>());

        result.add(dto);

        Mockito.when(dataSourceDefinitionService.findAll(query)).thenReturn(null);
        assertDoesNotThrow(databaseTypeSchedule::cleanUpForDatabaseTypes);
        verify(fileService, never()).deleteFileById(any());

        Mockito.when(dataSourceDefinitionService.findAll(query)).thenReturn(result);
        doNothing().when(fileService).deleteFileById(any());

        assertDoesNotThrow(databaseTypeSchedule::cleanUpForDatabaseTypes);

        ArgumentCaptor<ObjectId> captor = ArgumentCaptor.forClass(ObjectId.class);

        verify(fileService, times(5)).deleteFileById(captor.capture());

        List<ObjectId> ids = captor.getAllValues().stream().sorted().collect(Collectors.toList());

        assertEquals(originalIds.size(), ids.size());
        assertTrue(ids.containsAll(originalIds));
        assertTrue(originalIds.containsAll(ids));

    }
}
