package com.tapdata.tm.task.service.batchin;


import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

class ParseRelMigFileTest {
    @Test
    void testNew() {
        Assertions.assertDoesNotThrow(() -> new ParseRelMigFile(new ParseParam()) {
            @Override
            public List<TaskDto> parse() {
                return null;
            }
        });
    }
    @Nested
    class ParseRelMigFileAbstractTest {
        Map<String, Object> relMigInfo;
        String version;
        Map<String, Object> project;
        Map<String, Object> schema;
        List<Map<String, Object>> queries;
        ParseParam param;
        Map<String, Object> content;
        ParseRelMigFile parseRelMigFile;
        @BeforeEach
        void init() {
            parseRelMigFile = mock(ParseRelMigFile.class);
            relMigInfo = new HashMap<>();
            version = "1.2.0";
            project = new HashMap<>();
            schema = new HashMap<>();
            queries = new ArrayList<>();
            param = new ParseParam();
            content = new HashMap<>();
            parseRelMigFile.relMigInfo = relMigInfo;
            parseRelMigFile.version = version;
            parseRelMigFile.project = project;
            parseRelMigFile.schema = schema;
            parseRelMigFile.queries = queries;
            parseRelMigFile.param = param;
            parseRelMigFile.content = content;

        }
    }
}