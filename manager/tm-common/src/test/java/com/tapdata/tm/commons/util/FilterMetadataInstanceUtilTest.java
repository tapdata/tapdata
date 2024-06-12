package com.tapdata.tm.commons.util;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class FilterMetadataInstanceUtilTest {
    @Test
    void testFilterTypeRawAndPossibleDataTypeIsNull(){
        MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field();
        field1.setFieldName("test1");
        field1.setTapType("{\"type\":8}");
        field1.setDeleted(false);
        Field field2 = new Field();
        field2.setFieldName("test2");
        field2.setTapType("{\"type\":8}");
        field2.setDeleted(false);
        Field field3 = new Field();
        field3.setFieldName("test3");
        field3.setTapType("{\"type\":8}");
        field3.setDeleted(false);
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        metadataInstancesDto.setFields(fields);
        List<TableIndex> indices = new ArrayList<>();
        TableIndex tableIndex = new TableIndex();
        List<TableIndexColumn> columns = new ArrayList<>();
        TableIndexColumn column1 = new TableIndexColumn();
        column1.setColumnName("test1");
        TableIndexColumn column2 = new TableIndexColumn();
        column2.setColumnName("test2");
        columns.add(column1);
        columns.add(column2);
        tableIndex.setColumns(columns);
        indices.add(tableIndex);
        metadataInstancesDto.setIndices(indices);
        Map<String, PossibleDataTypes> dataTypes = new HashMap<>();
        dataTypes.put("test1",new PossibleDataTypes().dataType("String"));
        dataTypes.put("test3",new PossibleDataTypes());
        metadataInstancesDto.setFindPossibleDataTypes(dataTypes);
        try(MockedStatic<JSON> jsonMockedStatic = mockStatic(JSON.class)){
            jsonMockedStatic.when(()->JSON.parseObject("{\"type\":8}", TapType.class)).thenReturn(new TapString()).thenReturn(new TapRaw()).thenReturn(new TapString());
            FilterMetadataInstanceUtil.filterMetadataInstancesFields(metadataInstancesDto);
            Assertions.assertTrue(metadataInstancesDto.getFields().contains(field1));
        }

    }

    @Test
    void testDataTypesIsNull(){
        MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field();
        field1.setFieldName("test1");
        field1.setTapType("{\"type\":8}");
        field1.setDeleted(false);
        Field field2 = new Field();
        field2.setFieldName("test2");
        field2.setTapType("{\"type\":8}");
        field2.setDeleted(false);
        Field field3 = new Field();
        field3.setFieldName("test3");
        field3.setTapType("{\"type\":8}");
        field3.setDeleted(false);
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        metadataInstancesDto.setFields(fields);
        try(MockedStatic<JSON> jsonMockedStatic = mockStatic(JSON.class)){
            jsonMockedStatic.when(()->JSON.parseObject("{\"type\":8}",TapType.class)).thenReturn(new TapString()).thenReturn(new TapRaw()).thenReturn(new TapString());
            Assertions.assertTrue(metadataInstancesDto.getFields().contains(field1));
            Assertions.assertTrue(metadataInstancesDto.getFields().contains(field2));
            Assertions.assertTrue(metadataInstancesDto.getFields().contains(field3));
        }
    }

    @Test
    @DisplayName("convert TapTable Test Fields is null  ")
    void testFieldsIsNull(){
        MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        metadataInstancesDto.setFields(new ArrayList<>());
        try(MockedStatic<JSON> jsonMockedStatic = mockStatic(JSON.class)){
            jsonMockedStatic.when(()->JSON.parseObject("{\"type\":8}",TapType.class)).thenReturn(new TapString()).thenReturn(new TapRaw()).thenReturn(new TapString());
            Assertions.assertTrue(metadataInstancesDto.getFields().isEmpty());
        }

    }
}
