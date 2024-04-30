package com.tapdata.tm.task.service.batchin;


import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.dto.TablePathInfo;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParseRelMig13OrMoreImplTest {
    ParseRelMig13OrMoreImpl parseRelMig13OrMore;

    @BeforeEach
    void init() {
        parseRelMig13OrMore = mock(ParseRelMig13OrMoreImpl.class);
    }

    @Test
    void testNew() {
        Assertions.assertDoesNotThrow(() -> new ParseRelMig13OrMoreImpl(new ParseParam()));
    }
    @Nested
    class GetTablePathInfoTest {
        Map<String, Object> contentMapping;
        Map<String, Object> content;
        Map<String, Object> tables;
        Map<String, Object> tablePath;
        Map<String, Object> tableInfo;
        @BeforeEach
        void init() {
            contentMapping = mock(Map.class);
            parseRelMig13OrMore.project = mock(Map.class);
            content = mock(Map.class);
            tables = mock(Map.class);
            tablePath = mock(Map.class);
            tableInfo = mock(Map.class);
            when(parseRelMig13OrMore.getFromMap(contentMapping, KeyWords.TABLE)).thenReturn("tableId");
            when(parseRelMig13OrMore.getFromMap(parseRelMig13OrMore.project, KeyWords.CONTENT)).thenReturn(content);
            when(parseRelMig13OrMore.getFromMap(content, KeyWords.TABLES)).thenReturn(tables);
            when(parseRelMig13OrMore.getFromMap(tables, "tableId")).thenReturn(tablePath);
            when(parseRelMig13OrMore.getFromMap(tablePath, KeyWords.PATH)).thenReturn(tableInfo);
            when(tableInfo.get(KeyWords.DATABASE)).thenReturn("database");
            when(tableInfo.get(KeyWords.SCHEMA)).thenReturn("schema");
            when(tableInfo.get(KeyWords.TABLE)).thenReturn("table");
            when(parseRelMig13OrMore.parseMap(any())).thenCallRealMethod();
            when(parseRelMig13OrMore.getTablePathInfo(contentMapping)).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            TablePathInfo tablePathInfo = parseRelMig13OrMore.getTablePathInfo(contentMapping);
            Assertions.assertNotNull(tablePathInfo);
            Assertions.assertEquals("table", tablePathInfo.getTable());
            Assertions.assertEquals("schema", tablePathInfo.getSchema());
            Assertions.assertEquals("database", tablePathInfo.getDatabase());
            verify(parseRelMig13OrMore).getFromMap(contentMapping, KeyWords.TABLE);
            verify(parseRelMig13OrMore).getFromMap(parseRelMig13OrMore.project, KeyWords.CONTENT);
            verify(parseRelMig13OrMore).getFromMap(content, KeyWords.TABLES);
            verify(parseRelMig13OrMore).getFromMap(tables, "tableId");
            verify(parseRelMig13OrMore).getFromMap(tablePath, KeyWords.PATH);
            verify(tableInfo).get(KeyWords.DATABASE);
            verify(tableInfo).get(KeyWords.SCHEMA);
            verify(tableInfo).get(KeyWords.TABLE);
            verify(parseRelMig13OrMore, times(4)).parseMap(any());
        }

    }
}