package com.tapdata.tm.metadatadefinition.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.batchtags.service.BatchListTagService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.entity.MetadataDefinitionEntity;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.repository.MetadataDefinitionRepository;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.param.BatchApplyListTagsParam;
import com.tapdata.tm.utils.Lists;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MetadataDefinitionServiceTest {
    MetadataDefinitionService metadataDefinitionService;
    MongoTemplate mongoTemplate;

    @BeforeEach
    void init() {
        metadataDefinitionService = mock(MetadataDefinitionService.class);
        mongoTemplate = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(metadataDefinitionService, "mongoTemplate", mongoTemplate);
    }

    @Nested
    class BatchPushListTagsTest {
        BatchUpdateParam batchUpdateParam;
        @BeforeEach
        void init() {
            batchUpdateParam = mock(BatchUpdateParam.class);

            when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), any(Class.class))).thenReturn(mock(UpdateResult.class));
            when(batchUpdateParam.getId()).thenReturn(Lists.newArrayList());
            when(batchUpdateParam.getListtags()).thenReturn(Lists.newArrayList());
        }

        void doVerify(String tableName, int times) {
            when(metadataDefinitionService.batchPushListTags(tableName, batchUpdateParam)).thenCallRealMethod();
            metadataDefinitionService.batchPushListTags(tableName, batchUpdateParam);

            verify(batchUpdateParam, times(1)).getId();
            verify(batchUpdateParam, times(1)).getListtags();
            verify(mongoTemplate, times(times)).updateMulti(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testConnections() {
            doVerify("Connections", 1);
        }

        @Test
        void testTask() {
            doVerify("Task", 1);
        }

        @Test
        void testModules() {
            doVerify("Modules", 1);
        }

        @Test
        void testOther() {
            doVerify("", 0);
        }
    }

    @Nested
    class BatchApplyListTagsTest {
        BatchListTagService batchListTagService;
        UserDetail userDetail;

        @BeforeEach
        void init() {
            batchListTagService = new BatchListTagService();
            userDetail = mock(UserDetail.class);
            ReflectionTestUtils.setField(metadataDefinitionService, "batchListTagService", batchListTagService);
            doCallRealMethod().when(metadataDefinitionService).batchApplyListTags(anyString(), any(BatchApplyListTagsParam.class), any(UserDetail.class));
        }

        @Test
        void shouldReturnEmptyWhenParamInvalid() {
            assertTrue(metadataDefinitionService.batchApplyListTags("Task", null, userDetail).isEmpty());
            assertTrue(metadataDefinitionService.batchApplyListTags("Task", batchApplyParam(Collections.emptyList(), tagState("tag-a", "TagA", "all")), userDetail).isEmpty());
            assertTrue(metadataDefinitionService.batchApplyListTags("Task", batchApplyParam(Collections.singletonList("id-1")), userDetail).isEmpty());

            verify(mongoTemplate, never()).find(any(Query.class), any(Class.class));
        }

        @Test
        void shouldReturnEmptyWhenDesiredTagsOnlyContainSomeState() {
            BatchApplyListTagsParam param = batchApplyParam(Collections.singletonList(new ObjectId().toHexString()),
                    tagState("tag-a", "TagA", "some"));

            assertTrue(metadataDefinitionService.batchApplyListTags("Task", param, userDetail).isEmpty());

            verify(mongoTemplate, never()).find(any(Query.class), any(Class.class));
        }

        @Test
        void shouldApplyTagsForConnectionsUsingMapListTags() {
            ObjectId id = new ObjectId();
            DataSourceEntity entity = new DataSourceEntity();
            entity.setId(id);
            entity.setListtags(Arrays.asList(mapTag("tag-a", "TagA"), mapTag("tag-b", "TagB")));
            BatchApplyListTagsParam param = batchApplyParam(Collections.singletonList(id.toHexString()),
                    tagState("tag-b", "TagB", "none"),
                    tagState("tag-c", "TagC", "all"));
            when(mongoTemplate.find(any(Query.class), eq(DataSourceEntity.class))).thenReturn(Collections.singletonList(entity));

            List<String> result = metadataDefinitionService.batchApplyListTags("Connections", param, userDetail);

            assertEquals(Collections.singletonList(id.toHexString()), result);
            ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
            verify(mongoTemplate).updateFirst(any(Query.class), updateCaptor.capture(), eq(DataSourceEntity.class));
            List<Map<String, String>> updatedTags = getSetListTags(updateCaptor.getValue());
            assertEquals(Arrays.asList(mapTag("tag-a", "TagA"), mapTag("tag-c", "TagC")), updatedTags);
        }

        @Test
        void shouldApplyTagsForInspectUsingTagList() {
            ObjectId id = new ObjectId();
            InspectEntity entity = new InspectEntity();
            entity.setId(id);
            entity.setListtags(Arrays.asList(new Tag("tag-a", "TagA"), new Tag("tag-b", "TagB")));
            BatchApplyListTagsParam param = batchApplyParam(Collections.singletonList(id.toHexString()),
                    tagState("tag-a", "TagA", "none"),
                    tagState("tag-c", "TagC", "all"));
            when(mongoTemplate.find(any(Query.class), eq(InspectEntity.class))).thenReturn(Collections.singletonList(entity));

            List<String> result = metadataDefinitionService.batchApplyListTags("Inspect", param, userDetail);

            assertEquals(Collections.singletonList(id.toHexString()), result);
            ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
            verify(mongoTemplate).updateFirst(any(Query.class), updateCaptor.capture(), eq(InspectEntity.class));
            assertEquals(Arrays.asList(new Tag("tag-b", "TagB"), new Tag("tag-c", "TagC")), getSetListTags(updateCaptor.getValue()));
        }

        @Test
        void shouldSkipTaskUpdateWhenTagsAreNotChanged() {
            ObjectId id = new ObjectId();
            TaskEntity entity = new TaskEntity();
            entity.setId(id);
            entity.setListtags(Collections.singletonList(new Tag("tag-a", "TagA")));
            BatchApplyListTagsParam param = batchApplyParam(Collections.singletonList(id.toHexString()),
                    tagState("tag-a", "TagA", "all"));
            when(mongoTemplate.find(any(Query.class), eq(TaskEntity.class))).thenReturn(Collections.singletonList(entity));

            List<String> result = metadataDefinitionService.batchApplyListTags("Task", param, userDetail);

            assertTrue(result.isEmpty());
            verify(mongoTemplate, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void shouldReturnEmptyForUnsupportedTableName() {
            BatchApplyListTagsParam param = batchApplyParam(Collections.singletonList(new ObjectId().toHexString()),
                    tagState("tag-a", "TagA", "all"));

            assertTrue(metadataDefinitionService.batchApplyListTags("Unknown", param, userDetail).isEmpty());

            verify(mongoTemplate, never()).find(any(Query.class), any(Class.class));
        }

        private BatchApplyListTagsParam batchApplyParam(List<String> ids, BatchApplyListTagsParam.TagState... tags) {
            BatchApplyListTagsParam param = new BatchApplyListTagsParam();
            param.setIds(ids);
            param.setTags(Arrays.asList(tags));
            return param;
        }

        private BatchApplyListTagsParam.TagState tagState(String id, String value, String desired) {
            BatchApplyListTagsParam.TagState tagState = new BatchApplyListTagsParam.TagState();
            tagState.setId(id);
            tagState.setValue(value);
            tagState.setDesired(desired);
            return tagState;
        }

        private Map<String, String> mapTag(String id, String value) {
            Map<String, String> tag = new LinkedHashMap<>();
            tag.put("id", id);
            tag.put("value", value);
            return tag;
        }

        @SuppressWarnings("unchecked")
        private <T> List<T> getSetListTags(Update update) {
            Document updateObject = update.getUpdateObject();
            return (List<T>) ((Document) updateObject.get("$set")).get("listtags");
        }
    }

    @Nested
    class FindTest{
        SettingsService settingsService;
        MetadataDefinitionRepository metadataDefinitionRepository;
        MetadataDefinitionService metadataDefinitionService;
        @BeforeEach
        void init() {
            settingsService = mock(SettingsService.class);
            metadataDefinitionRepository = mock(MetadataDefinitionRepository.class);
            metadataDefinitionService = new MetadataDefinitionService(metadataDefinitionRepository);
            ReflectionTestUtils.setField(metadataDefinitionService, "settingsService", settingsService);
        }
        @Test
        void testCloud() {
            when(settingsService.isCloud()).thenReturn(true);
            Filter filter = new Filter();
            metadataDefinitionService.find(filter,mock(UserDetail.class));
            verify(metadataDefinitionRepository,times(1)).findAll(any(Filter.class),any(UserDetail.class));
        }

        @Test
        void testDass() {
            when(settingsService.isCloud()).thenReturn(false);
            Filter filter = new Filter();
            metadataDefinitionService.find(filter,mock(UserDetail.class));
            verify(metadataDefinitionRepository,times(1)).findAll(any(Filter.class), any(UserDetail.class));
        }
        @Test
        void testDassTag() {
            when(settingsService.isCloud()).thenReturn(false);
            Filter filter = new Filter();
            Where where = new Where();
            HashMap itemMap = new HashMap();
            itemMap.put("item_type", "dataflow");
            where.put("or", com.tapdata.tm.utils.Lists.of(itemMap));
            filter.setWhere(where);
            metadataDefinitionService.find(filter,mock(UserDetail.class));
            verify(metadataDefinitionRepository,times(1)).findAll(any(Filter.class));
        }
        @Test
        void testCloudTag() {
            when(settingsService.isCloud()).thenReturn(true);
            Filter filter = new Filter();
            Where where = new Where();
            HashMap itemMap = new HashMap();
            itemMap.put("item_type", "dataflow");
            where.put("or", com.tapdata.tm.utils.Lists.of(itemMap));
            filter.setWhere(where);
            metadataDefinitionService.find(filter,mock(UserDetail.class));
            verify(metadataDefinitionRepository,times(1)).findAll(any(Filter.class), any(UserDetail.class));
        }
    }

    @Nested
    class deleteByIdTest {
        @Test
        @DisplayName("test for metadataDefinitionDto is null")
        void test1() {
            ObjectId id = mock(ObjectId.class);
            UserDetail user = mock(UserDetail.class);
            when(metadataDefinitionService.findById(id, user)).thenReturn(null);
            doCallRealMethod().when(metadataDefinitionService).deleteById(id, user);
            assertThrows(BizException.class, () -> metadataDefinitionService.deleteById(id, user));
        }
    }
    @Nested
    class saveTest {
        MetadataDefinitionDto metadataDefinitionDto;
        UserDetail userDetail;
        MetadataDefinitionRepository repository;
        @BeforeEach
        void beforeEach() {
            metadataDefinitionDto = mock(MetadataDefinitionDto.class);
            userDetail = mock(UserDetail.class);
            repository = mock(MetadataDefinitionRepository.class);
            ReflectionTestUtils.setField(metadataDefinitionService, "repository", repository);
            doCallRealMethod().when(metadataDefinitionService).save(metadataDefinitionDto, userDetail);
        }
        @Test
        @DisplayName("test save when user is admin and parent is null")
        void test1() {
            ObjectId id = mock(ObjectId.class);
            when(metadataDefinitionDto.getId()).thenReturn(id);
            when(userDetail.isRoot()).thenReturn(true);
            when(metadataDefinitionService.findById(id, userDetail)).thenReturn(metadataDefinitionDto);
            String parentId = "671b045dfdd7620f87d3d1a6";
            when(metadataDefinitionDto.getParent_id()).thenReturn(parentId);
            assertThrows(BizException.class, ()->metadataDefinitionService.save(metadataDefinitionDto, userDetail));
        }
        @Test
        @DisplayName("test save when user is admin and id is null")
        void test2() {
            when(metadataDefinitionDto.getId()).thenReturn(null);
            when(userDetail.isRoot()).thenReturn(true);
            String parentId = "671b045dfdd7620f87d3d1a6";
            when(metadataDefinitionDto.getParent_id()).thenReturn(parentId);
            when(metadataDefinitionService.findById(any(ObjectId.class), any(UserDetail.class))).thenReturn(metadataDefinitionDto);
            when(repository.save(eq(null), any(UserDetail.class))).thenReturn(mock(MetadataDefinitionEntity.class));
            MetadataDefinitionDto actual = metadataDefinitionService.save(metadataDefinitionDto, userDetail);
            assertNotNull(actual);
        }
        @Test
        @DisplayName("test save when user is admin and exsitedOne is null")
        void test3() {
            when(metadataDefinitionDto.getId()).thenReturn(mock(ObjectId.class));
            when(userDetail.isRoot()).thenReturn(true);
            when(metadataDefinitionService.findById(any(ObjectId.class), any(UserDetail.class))).thenReturn(null);
            assertThrows(BizException.class, ()->metadataDefinitionService.save(metadataDefinitionDto, userDetail));
        }
    }

    @Nested
    @DisplayName("Task Priority Ordering Tests")
    class TaskPriorityOrderingTest {

        @BeforeEach
        void setUp() {
            doCallRealMethod().when(metadataDefinitionService).orderTaskByTagPriority(any());
        }

        @Test
        @DisplayName("Should return empty list when input is null")
        void testOrderTaskByTagPriority_NullInput() {
            // When
            List<TaskDto> result = metadataDefinitionService.orderTaskByTagPriority(null);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when input is empty")
        void testOrderTaskByTagPriority_EmptyInput() {
            // Given
            List<TaskDto> emptyTasks = Collections.emptyList();

            // When
            List<TaskDto> result = metadataDefinitionService.orderTaskByTagPriority(emptyTasks);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return copy of original list when no tag definitions found")
        void testOrderTaskByTagPriority_NoTagDefinitions() {
            // Given
            List<TaskDto> tasks = createMockTasks();
            when(metadataDefinitionService.findAll(any(Query.class)))
                .thenReturn(Collections.emptyList());

            // When
            List<TaskDto> result = metadataDefinitionService.orderTaskByTagPriority(tasks);

            // Then
            assertNotNull(result);
            assertEquals(tasks.size(), result.size());
            assertNotSame(tasks, result); // Should be a copy
            verify(metadataDefinitionService).findAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle exception gracefully and return original order")
        void testOrderTaskByTagPriority_ExceptionHandling() {
            // Given
            List<TaskDto> tasks = createMockTasks();
            when(metadataDefinitionService.findAll(any(Query.class)))
                .thenThrow(new RuntimeException("Database error"));

            // When
            List<TaskDto> result = metadataDefinitionService.orderTaskByTagPriority(tasks);

            // Then
            assertNotNull(result);
            assertEquals(tasks.size(), result.size());
            // Should return a copy even when exception occurs
            assertNotSame(tasks, result);
        }

        @Test
        @DisplayName("Should handle tasks with no tags")
        void testOrderTaskByTagPriority_TasksWithNoTags() {
            // Given
            List<TaskDto> tasks = createTasksWithoutTags();

            when(metadataDefinitionService.findAll(any(Query.class)))
                .thenReturn(createMockTagDefinitions());

            // When
            List<TaskDto> result = metadataDefinitionService.orderTaskByTagPriority(tasks);

            // Then
            assertNotNull(result);
            assertEquals(tasks.size(), result.size());
        }

        @Test
        @DisplayName("Should handle mixed tasks with and without tags")
        void testOrderTaskByTagPriority_MixedTasks() {
            // Given
            List<TaskDto> tasks = createMixedTasks();

            when(metadataDefinitionService.findAll(any(Query.class)))
                .thenReturn(createMockTagDefinitions());

            // When
            List<TaskDto> result = metadataDefinitionService.orderTaskByTagPriority(tasks);

            // Then
            assertNotNull(result);
            assertEquals(tasks.size(), result.size());
            assertEquals("task2", result.get(0).getName());
            assertEquals("task1", result.get(1).getName());
            assertEquals("task3", result.get(2).getName());
            assertEquals("task4", result.get(3).getName());
        }


        private List<TaskDto> createMockTasks() {
            List<TaskDto> tasks = new ArrayList<>();

            TaskDto task1 = new TaskDto();
            task1.setId(new ObjectId());
            task1.setListtags(Arrays.asList(new Tag("tag1", "Tag 1")));

            TaskDto task2 = new TaskDto();
            task2.setId(new ObjectId());
            task2.setListtags(Arrays.asList(new Tag("tag2", "Tag 2")));

            tasks.add(task1);
            tasks.add(task2);

            return tasks;
        }

        private List<TaskDto> createTasksWithoutTags() {
            List<TaskDto> tasks = new ArrayList<>();

            TaskDto task1 = new TaskDto();
            task1.setId(new ObjectId());
            task1.setListtags(Collections.emptyList());

            TaskDto task2 = new TaskDto();
            task2.setId(new ObjectId());
            task2.setListtags(null);

            tasks.add(task1);
            tasks.add(task2);

            return tasks;
        }

        private List<TaskDto> createMixedTasks() {
            List<TaskDto> tasks = new ArrayList<>();

            TaskDto task1 = new TaskDto();
            task1.setName("task1");
            task1.setId(new ObjectId());
            task1.setListtags(Arrays.asList(
                    new Tag("68ae6d12539cddb2ead83597", "tag1"), new Tag("68ae6d12539cddb2ead83598", "tag2")
            ));

            TaskDto task2 = new TaskDto();
            task2.setId(new ObjectId());
            task2.setListtags(Arrays.asList(
                    new Tag("68aff2dce791d10b3e148b44", "tag3"), new Tag("68ae6d12539cddb2ead83599", "tag4")
            ));
            task2.setName("task2");


            TaskDto task3 = new TaskDto();
            task3.setId(new ObjectId());
            task3.setListtags(Arrays.asList(
                    new Tag("68ae6d12539cddb2ead83480", "tag5")
            ));
            task3.setName("task3");


            TaskDto task4 = new TaskDto();
            task4.setId(new ObjectId());
            task4.setListtags(Arrays.asList(
                    new Tag("68ae6d12539cddb2ead83482", "tag7")
            ));
            task4.setName("task4");

            tasks.add(task1);
            tasks.add(task2);
            tasks.add(task3);
            tasks.add(task4);

            return tasks;
        }


        private List<MetadataDefinitionDto> createMockTagDefinitions() {
            List<MetadataDefinitionDto> tagDefinitions = new ArrayList<>();
            MetadataDefinitionDto tag1 = new MetadataDefinitionDto();
            tag1.setId(new ObjectId("68ae6d12539cddb2ead83597"));
            tag1.setPriority(4);
            tag1.setParent_id(null);
            tag1.setValue("tag1");
            tagDefinitions.add(tag1);


            MetadataDefinitionDto tag2 = new MetadataDefinitionDto();
            tag2.setId(new ObjectId("68ae6d12539cddb2ead83598"));
            tag2.setPriority(5);
            tag2.setParent_id(null);
            tag2.setValue("tag2");
            tagDefinitions.add(tag2);


            MetadataDefinitionDto tagParent = new MetadataDefinitionDto();
            tagParent.setId(new ObjectId("68ae6d12539cddb2ead83596"));
            tagParent.setPriority(1);
            tagParent.setParent_id(null);
            tagParent.setValue("tagParent");
            tagDefinitions.add(tagParent);

            MetadataDefinitionDto tag3 = new MetadataDefinitionDto();
            tag3.setId(new ObjectId("68aff2dce791d10b3e148b44"));
            tag3.setPriority(2);
            tag3.setParent_id("68ae6d12539cddb2ead83596");
            tag3.setValue("tag3");

            tagDefinitions.add(tag3);
            MetadataDefinitionDto tag4 = new MetadataDefinitionDto();
            tag4.setId(new ObjectId("68ae6d12539cddb2ead83599"));
            tag4.setPriority(3);
            tag4.setParent_id("68ae6d12539cddb2ead83596");
            tag4.setValue("tag4");
            tagDefinitions.add(tag4);

            MetadataDefinitionDto tag5 = new MetadataDefinitionDto();
            tag5.setId(new ObjectId("68ae6d12539cddb2ead83480"));
            tag5.setParent_id(null);
            tag5.setValue("tag5");
            tagDefinitions.add(tag5);

            MetadataDefinitionDto tag6 = new MetadataDefinitionDto();
            tag6.setId(new ObjectId("68ae6d12539cddb2ead83481"));
            tag6.setParent_id(null);
            tag6.setValue("tag6");
            tagDefinitions.add(tag6);

            MetadataDefinitionDto tag7 = new MetadataDefinitionDto();
            tag7.setId(new ObjectId("68ae6d12539cddb2ead83482"));
            tag7.setParent_id("68ae6d12539cddb2ead83481");
            tag7.setPriority(1);
            tag7.setValue("tag7");
            tagDefinitions.add(tag7);


            return tagDefinitions;
        }
    }



}
