package com.tapdata.tm.utils;

import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;


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
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
        MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
        List<String> result = MetadataInstancesFilterUtil.getFilteredOriginalNames(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3),databaseNode);
        Assertions.assertEquals(1,result.size());
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
        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
        Field field2 = new Field();
        field2.setPrimaryKey(false);
        metadataInstancesDto2.setOriginalName("test2");
        metadataInstancesDto2.setFields(Arrays.asList(field2));
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
}
