package com.tapdata.tm.utils;

import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import io.tapdata.entity.schema.partition.TapPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MetadataInstancesFilterUtilTest {
    @Test
    void test_getFilteredOriginalNames(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        metadataInstancesDto2.setOriginalName("test2");
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        List<String> result = MetadataInstancesFilterUtil.getFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(2,result.size());
    }

    @Test
    void test_getFilteredOriginalNames_tableExpression_isNull(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        metadataInstancesDto2.setOriginalName("test2");
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        List<String> result = MetadataInstancesFilterUtil.getFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(0,result.size());
    }

    @Test
    void test_getFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_HasKeys(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("HasKeys");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        TableIndex index1 = new TableIndex();
        index1.setUnique(true);
        metadataInstancesDto1.setIndices(Arrays.asList(index1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        TableIndex index2 = new TableIndex();
        index2.setUnique(true);
        metadataInstancesDto2.setIndices(Arrays.asList(index2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        List<String> result = MetadataInstancesFilterUtil.getFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(2,result.size());
    }

    @Test
    void test_getFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_NoKeys(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("NoKeys");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        TableIndex index1 = new TableIndex();
        index1.setUnique(true);
        metadataInstancesDto1.setIndices(Arrays.asList(index1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        TableIndex index2 = new TableIndex();
        index2.setUnique(false);
        metadataInstancesDto2.setIndices(Arrays.asList(index2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        List<String> result = MetadataInstancesFilterUtil.getFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(1,result.size());
    }

    @Test
    void test_getFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_All(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("All");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        List<String> result = MetadataInstancesFilterUtil.getFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(2,result.size());
    }

    @Test
    void test_countFilteredOriginalNames(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        metadataInstancesDto2.setOriginalName("test2");
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(2,result);
    }

    @Test
    void test_countFilteredOriginalNames_tableExpression_isNull(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        metadataInstancesDto2.setOriginalName("test2");
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(0,result);
    }

    @Test
    void test_countFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_HasKeys(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("HasKeys");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(1,result);
    }

    @Test
    void test_countFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_NoKeys(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("NoKeys");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        TableIndex index1 = new TableIndex();
        index1.setUnique(true);
        metadataInstancesDto1.setIndices(Arrays.asList(index1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        TableIndex index2 = new TableIndex();
        index2.setUnique(false);
        metadataInstancesDto2.setIndices(Arrays.asList(index2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(1,result);
    }

    @Test
    void test_countFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_All(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("All");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(2,result);
    }

    @Test
    void test_countFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_OnlyPrimaryKey(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("OnlyPrimaryKey");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        TableIndex index1 = new TableIndex();
        index1.setUnique(true);
        metadataInstancesDto1.setIndices(Arrays.asList(index1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(true);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        TableIndex index2 = new TableIndex();
        index2.setUnique(false);
        metadataInstancesDto2.setIndices(Arrays.asList(index2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        MetadataInstancesDto metadataInstancesDto4 = new MetadataInstancesDto();
        Field field4 = new Field();
        field4.setPrimaryKey(false);
        metadataInstancesDto4.setOriginalName("test4");
        metadataInstancesDto4.setFields(Arrays.asList(field4));
        MetadataInstancesDto metadataInstancesDto5 = new MetadataInstancesDto();
        Field field5 = new Field();
        field5.setPrimaryKey(true);
        metadataInstancesDto5.setOriginalName("test5");
        metadataInstancesDto5.setFields(Arrays.asList(field5));
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3,metadataInstancesDto4,metadataInstancesDto5),databaseNode);
        Assertions.assertEquals(1,result);
    }

    @Test
    void test_countFilteredOriginalNames_NoPrimaryKeyTableSelectType_is_OnlyUniqueIndex(){
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setMigrateTableSelectType("expression");
        databaseNode.setTableExpression("test.*");
        databaseNode.setNoPrimaryKeyTableSelectType("OnlyUniqueIndex");
        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
        metadataInstancesDto1.setOriginalName("test1");
        Field field1 = new Field();
        field1.setPrimaryKey(true);
        metadataInstancesDto1.setFields(Arrays.asList(field1));
        TableIndex index1 = new TableIndex();
        index1.setUnique(true);
        metadataInstancesDto1.setIndices(Arrays.asList(index1));
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        TableIndex index2 = new TableIndex();
        index2.setUnique(true);
        metadataInstancesDto2.setIndices(Arrays.asList(index2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        MetadataInstancesDto metadataInstancesDto4 = new MetadataInstancesDto();
        Field field4 = new Field();
        field4.setPrimaryKey(false);
        metadataInstancesDto4.setOriginalName("test4");
        metadataInstancesDto4.setFields(Arrays.asList(field4));
        TableIndex index4 = new TableIndex();
        index4.setUnique(false);
        metadataInstancesDto4.setIndices(Arrays.asList(index4));
        MetadataInstancesDto metadataInstancesDto5 = new MetadataInstancesDto();
        Field field5 = new Field();
        field5.setPrimaryKey(false);
        metadataInstancesDto5.setOriginalName("test5");
        metadataInstancesDto5.setFields(Arrays.asList(field5));
        Long result = MetadataInstancesFilterUtil.countFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3,metadataInstancesDto4,metadataInstancesDto5),databaseNode);
        Assertions.assertEquals(1,result);
    }

    @Nested
    class filterBySyncSourcePartitionTableEnable {
        DatabaseNode sourceNode;
        List<MetadataInstancesDto> dtos;

        @Test
        void testSyncSourcePartitionTableEnable() {
            sourceNode = mock(DatabaseNode.class);
            MetadataInstancesDto dto = mock(MetadataInstancesDto.class);
            MetadataInstancesDto meta = mock(MetadataInstancesDto.class);
            MetadataInstancesDto meta1 = mock(MetadataInstancesDto.class);
            dtos = new ArrayList<>();
            dtos.add(dto);
            dtos.add(meta);
            dtos.add(meta1);

            when(dto.getPartitionInfo()).thenReturn(null);

            when(meta.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(meta.getName()).thenReturn("name");
            when(meta.getPartitionMasterTableId()).thenReturn("id");

            when(meta1.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(meta1.getName()).thenReturn("name");
            when(meta1.getPartitionMasterTableId()).thenReturn("name");

            when(sourceNode.getSyncSourcePartitionTableEnable()).thenReturn(true);

            List<MetadataInstancesDto> metadataInstancesDtos = MetadataInstancesFilterUtil.filterBySyncSourcePartitionTableEnable(sourceNode, dtos);
            Assertions.assertNotNull(metadataInstancesDtos);
            Assertions.assertEquals(ArrayList.class.getName(), metadataInstancesDtos.getClass().getName());
            Assertions.assertEquals(2, metadataInstancesDtos.size());
        }
        @Test
        void testNotSyncSourcePartitionTableEnable() {
            sourceNode = mock(DatabaseNode.class);
            MetadataInstancesDto dto = mock(MetadataInstancesDto.class);
            MetadataInstancesDto meta = mock(MetadataInstancesDto.class);
            MetadataInstancesDto meta1 = mock(MetadataInstancesDto.class);
            dtos = new ArrayList<>();
            dtos.add(dto);
            dtos.add(meta);
            dtos.add(meta1);

            when(dto.getPartitionInfo()).thenReturn(null);

            when(meta.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(meta.getName()).thenReturn("name");
            when(meta.getPartitionMasterTableId()).thenReturn("id");

            when(meta1.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(meta1.getName()).thenReturn("name");
            when(meta1.getPartitionMasterTableId()).thenReturn("name");

            when(sourceNode.getSyncSourcePartitionTableEnable()).thenReturn(false);

            List<MetadataInstancesDto> metadataInstancesDtos = MetadataInstancesFilterUtil.filterBySyncSourcePartitionTableEnable(sourceNode, dtos);
            Assertions.assertNotNull(metadataInstancesDtos);
            Assertions.assertEquals(ArrayList.class.getName(), metadataInstancesDtos.getClass().getName());
            Assertions.assertEquals(2, metadataInstancesDtos.size());
        }
        @Test
        void testNullSyncSourcePartitionTableEnable() {
            sourceNode = mock(DatabaseNode.class);
            MetadataInstancesDto dto = mock(MetadataInstancesDto.class);
            dtos = new ArrayList<>();
            dtos.add(dto);

            when(dto.getPartitionInfo()).thenReturn(null);
            when(sourceNode.getSyncSourcePartitionTableEnable()).thenReturn(null);

            List<MetadataInstancesDto> metadataInstancesDtos = MetadataInstancesFilterUtil.filterBySyncSourcePartitionTableEnable(sourceNode, dtos);
            Assertions.assertNotNull(metadataInstancesDtos);
            Assertions.assertEquals(ArrayList.class.getName(), metadataInstancesDtos.getClass().getName());
            Assertions.assertEquals(1, metadataInstancesDtos.size());
        }
    }
}
