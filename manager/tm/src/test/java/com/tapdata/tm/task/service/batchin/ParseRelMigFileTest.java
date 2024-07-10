package com.tapdata.tm.task.service.batchin;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.dto.TablePathInfo;
import com.tapdata.tm.task.service.batchin.entity.GenericPropertiesParam;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.junit.jupiter.api.*;
import org.springframework.mock.web.MockMultipartFile;

import java.io.FileInputStream;
import java.net.URL;
import java.util.*;

import static com.tapdata.tm.task.service.batchin.ParseRelMig.RM_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class ParseRelMigFileTest {
    ParseRelMigFile parseRelMigFile;
    FileInputStream fileInputStream;
    MockMultipartFile mockMultipartFile;
    String rmJson;
    @BeforeEach
    void beforeEach() throws Exception {
        URL resource = this.getClass().getClassLoader().getResource("test.relmig");
        fileInputStream = new FileInputStream(resource.getFile());
        mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
        rmJson = new String(mockMultipartFile.getBytes());

        HashMap<String, Object> rmProject = new ObjectMapper().readValue(rmJson, HashMap.class);
        ParseParam param = new ParseParam()
                .withMultipartFile(mockMultipartFile);
        param.setRelMigStr(rmJson);
        param.setRelMigInfo(rmProject);
        parseRelMigFile = spy(new ParseRelMigFile(param) {
            @Override
            public List<TaskDto> parse() {
                return null;
            }
        });
    }
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
    @Nested
    class scanOneChildrenTest{
        String parentId = "1334091b-5ca0-4ad1-bd0b-c461d30b80ee";
        String childId = "559c3fe5-d721-4218-8bcc-0ee6a0545e02";
        GenericPropertiesParam propertiesParam;
        @BeforeEach
        void beforeEach(){
            Map<String, Object> full = (Map<String, Object>) parseRelMigFile.schema.get("full");
            Map<String, Object> contentMapping = (Map<String, Object>) parseRelMigFile.content.get("mappings");
            Map<String, Object> relationships = (Map<String, Object>) parseRelMigFile.content.get("relationships");
            Map<String, Object> relationshipsMapping = (Map<String, Object>) relationships.get("mappings");
            propertiesParam = GenericPropertiesParam.of()
                    .withContentMapping(contentMapping)
                    .withRelationshipsMapping(relationshipsMapping)
                    .withFull(full)
                    .withSourceToJS(new HashMap<>())
                    .withRenameFields(new HashMap<>());
        }
        @Test
        @DisplayName("test for embedded document array when parent target path is empty")
        void test1(){
            Map<String, Object> rootProperties = new HashMap<>();
            rootProperties.put(KeyWords.TARGET_PATH, KeyWords.EMPTY);
            rootProperties.put(RM_ID, parentId);
            propertiesParam.withParent(rootProperties);
            doNothing().when(parseRelMigFile).scanAllFieldKeys(any(),anyMap(),anyMap());
            Map<String, Object> actual = parseRelMigFile.scanOneChildren(propertiesParam, childId);
            assertEquals("direct",actual.get("targetPath"));
        }
        @Test
        @DisplayName("test for embedded document array when parent target path is not empty")
        void test2(){Map<String, Object> rootProperties = new HashMap<>();
            rootProperties.put(KeyWords.TARGET_PATH, "parent");
            rootProperties.put(RM_ID, parentId);
            propertiesParam.withParent(rootProperties);
            doNothing().when(parseRelMigFile).scanAllFieldKeys(any(),anyMap(),anyMap());
            Map<String, Object> actual = parseRelMigFile.scanOneChildren(propertiesParam, childId);
            assertEquals("parent.direct",actual.get("targetPath"));
        }
    }
    @Nested
    class getTableSchemaTest{
        @Test
        void testGetTableSchemaSimple(){
            Map<String, Object> full = (Map<String, Object>) parseRelMigFile.schema.get("full");
            Map<String, Object> mappings = (Map<String, Object>) parseRelMigFile.content.get("mappings");
            Map<String, Object> contentMapping = (Map<String, Object>) mappings.get("1334091b-5ca0-4ad1-bd0b-c461d30b80ee");
            TablePathInfo tableInfo = parseRelMigFile.getTablePathInfo(contentMapping);
            Map<String, Object> actual = parseRelMigFile.getTableSchema(full, tableInfo);
            assertNotNull(actual);
        }
    }
}