package com.tapdata.tm.ds.service.impl;

import cn.hutool.core.date.DateUtil;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.ds.dto.PdkVersionCheckDto;
import com.tapdata.tm.ds.vo.PdkFileTypeEnum;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.SneakyThrows;
import org.apache.commons.fileupload2.core.FileItem;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class PkdSourceServiceTest {
    private PkdSourceService pkdSourceService;
    private FileService fileService;
    private DataSourceDefinitionService dataSourceDefinitionService;

    @BeforeEach
    void buildMember(){
        pkdSourceService = new PkdSourceService();
        fileService = mock(FileService.class);
        dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
        ReflectionTestUtils.setField(pkdSourceService,"fileService",fileService);
        ReflectionTestUtils.setField(pkdSourceService,"dataSourceDefinitionService",dataSourceDefinitionService);
    }
    @Nested
    class uploadPdkTest{
        @Test
        @SneakyThrows
        void testUploadPdk(){
            MultipartFile[] files = new MultipartFile[1];
            MultipartFile file = mock(MultipartFile.class);
            files[0] = file;
            List<PdkSourceDto > pdkSourceDtos = new ArrayList<>();
            PdkSourceDto pdkSourceDto = mock(PdkSourceDto.class);
            pdkSourceDtos.add(pdkSourceDto);
            boolean latest = false;
            UserDetail user = mock(UserDetail.class);
            FileItem item = mock(FileItem.class);
            when(pdkSourceDto.getVersion()).thenReturn("1.0-SNAPSHOT");
            when(file.getOriginalFilename()).thenReturn("a.jar");
            when(file.getName()).thenReturn("a.jar");
            InputStream ins = mock(InputStream.class);
            when(file.getInputStream()).thenReturn(ins);
            when(ins.read(any())).thenReturn(-1);
            when(fileService.storeFile(any(),anyString(),any(),anyMap())).thenReturn(mock(ObjectId.class));
            pkdSourceService.uploadPdk(files,pdkSourceDtos,latest,user);
            verify(fileService).storeFile(any(),anyString(),any(),anyMap());
            FileUtils.deleteQuietly(new File("a.jar"));

            GridFSFindIterable result = mock(GridFSFindIterable.class);

            DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
            dto.setId(new ObjectId());
            dto.setJarRid(new ObjectId().toHexString());
            dto.setIcon(new ObjectId().toHexString());
            when(dataSourceDefinitionService.findOne(any())).thenReturn(dto);
            when(fileService.find(any())).thenReturn(result);
            doAnswer(answer -> {

                Consumer consumer = answer.getArgument(0);
                GridFSFile gridFSFile = mock(GridFSFile.class);
                when(gridFSFile.getObjectId()).thenReturn(new ObjectId());
                consumer.accept(gridFSFile);

                return null;
            }).when(result).forEach(any());
            pkdSourceService.uploadPdk(files, pdkSourceDtos, true, user);
            verify(fileService).scheduledDeleteFiles(any(), anyString(), anyString(), any(), any());

            /*pkdSourceService.uploadPdk(files, pdkSourceDtos, true, user);
            verify(fileService).scheduledDeleteFiles(any(), anyString(), anyString(), any(), any());*/

        }
    }
    @Nested
    class checkJarMD5{
        @Test
        void testCheckJarMD5(){
            Criteria criteria = Criteria.where("metadata.pdkHash").is("111");
            Query query = new Query(criteria);
            criteria.and("metadata.pdkAPIBuildNumber").lte(14);
            query.with(Sort.by("metadata.pdkAPIBuildNumber").descending());
            GridFSFile gridFSFile = mock(GridFSFile.class);
            Document document = new Document();
            document.append("md5","123456");
            when(gridFSFile.getMetadata()).thenReturn(document);
            when(fileService.findOne(query)).thenReturn(gridFSFile);
            String actual = pkdSourceService.checkJarMD5("111", 14);
            assertEquals("123456",actual);
        }
        @Test
        void testCheckJarMd5CompatibleOldEngine(){
            Criteria criteria = Criteria.where("metadata.pdkHash").is("111").and("filename").is("a.jar");
            Query query = new Query(criteria);
            GridFSFile gridFSFile = mock(GridFSFile.class);
            Document document = new Document();
            document.append("md5","123456");
            when(gridFSFile.getMetadata()).thenReturn(document);
            when(fileService.findOne(query)).thenReturn(gridFSFile);
            String actual = pkdSourceService.checkJarMD5("111", "a.jar");
            assertEquals("123456",actual);
        }
        @Test
        void testCheckJarMD5WithFileName(){
            String fileName = "a.jar";
            Criteria criteria = Criteria.where("metadata.pdkHash").is("111");
            Query query = new Query(criteria);
            criteria.and("metadata.pdkAPIBuildNumber").lte(14);
            criteria.and("filename").is(fileName);
            query.with(Sort.by("metadata.pdkAPIBuildNumber").descending().and(Sort.by("uploadDate").descending()));
            GridFSFile gridFSFile = mock(GridFSFile.class);
            Document document = new Document();
            document.append("md5","123456");
            when(gridFSFile.getMetadata()).thenReturn(document);
            when(fileService.findOne(query)).thenReturn(gridFSFile);
            String actual = pkdSourceService.checkJarMD5("111", 14, fileName);
            assertEquals("123456",actual);
        }
    }

    @Nested
    class UploadDocsTest {
        Map<String, MultipartFile> docMap;
        LinkedHashMap<String, Object> messages;
        Map<String, Object> fileInfo;
        Map<String, Object> oemConfig;

        @BeforeEach
        void setUp() {
            docMap = new LinkedHashMap<>();
            messages = new LinkedHashMap<>();
            fileInfo = new LinkedHashMap<>();
            oemConfig = new LinkedHashMap<>();
        }

        @Test
        void testNullParams() {
            String filePath = "docs/test_en_US.md";

            // mock data
            MultipartFile file = mock(MultipartFile.class);
            docMap.put(filePath, file);
            messages.put("zh_CN", null);
            messages.put("en_US", new HashMap<String, String>() {{
                put("null", null); // test path is null
                put("not_start_doc", filePath); // test key not doc
            }});

            // docMap is null
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(null, messages, fileInfo, oemConfig));
            // docMap is empty
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(new LinkedHashMap<>(), messages, fileInfo, oemConfig));
            // messages is null
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, null, fileInfo, oemConfig));
            // messages lang is null
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, messages, fileInfo, oemConfig));

            // Verification results
            verify(fileService, times(0)).storeFile(any(), anyString(), any(), anyMap());
        }

        @Test
        void testReadmeDoc() {
            String filePath = "docs/test_en_US.md";

            // mock data
            MultipartFile file = mock(MultipartFile.class);
            when(file.getOriginalFilename()).thenReturn(filePath);
            docMap.put(filePath, file);
            messages.put("default", "en_US");
            messages.put("en_US", new HashMap<String, String>() {{
                put("doc", filePath);
            }});

            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, messages, fileInfo, oemConfig));

            // Verification results
            verify(fileService, times(1)).storeFile(any(), anyString(), any(), anyMap());
        }

        @Test
        void testDocTips() {
            String filePath = "docs/test_en_US.md";

            // mock data
            MultipartFile file = mock(MultipartFile.class);
            when(file.getOriginalFilename()).thenReturn(filePath);
            docMap.put(filePath, file);
            messages = new LinkedHashMap<String, Object>(){{
                put("default", "en_US");
                put("en_US", new HashMap<String, String>() {{
                    put("doc:test", filePath);
                }});
                put("zh_CN", new HashMap<String, String>() {{
                    put("doc:test", filePath);
                }});
            }};

            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, messages, fileInfo, oemConfig));

            // Verification results
            verify(fileService, times(1)).storeFile(any(), anyString(), any(), anyMap());
        }
    }

    @Nested
    class DownloadDocTest {

        String customerId = "test-customer-id";
        String pdkHash;
        Integer pdkBuildNumber;
        String filename;
        UserDetail user;
        HttpServletResponse response;

        @BeforeEach
        void setUp() {
            pdkHash = "123456";
            pdkBuildNumber = 1;
            filename = "test_en_US.md";
            user = mock(UserDetail.class);
            when(user.getCustomerId()).thenReturn(customerId);
            response = mock(HttpServletResponse.class);
        }

        @Test
        void testNotfoundDatasource() {
            pdkBuildNumber = null; // test pdkBuildNumber not add to criteria

            // mock data
            doAnswer(invocation -> {
                Query query = invocation.getArgument(0);
                assertNotNull(query);
                Document doc = query.getQueryObject();
                assertNotNull(doc);

                // Verification query prams
                assertTrue(doc.containsKey("pdkHash"));
                assertFalse(doc.containsKey("pdkAPIBuildNumber"));
                return null;
            }).when(dataSourceDefinitionService).findOne(any(Query.class));

            assertDoesNotThrow(() -> {
                pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response);

                // Verification results
                verify(response, times(1)).sendError(eq(404));
            });
        }

        @Test
        void testNotInScope() {
            // mock data
            DataSourceDefinitionDto sourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            when(sourceDefinitionDto.getScope()).thenReturn("customer");
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(sourceDefinitionDto);

            assertThrows(BizException.class, () -> pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response));
        }

        @Test
        void testIsCustomScope() {
            // mock data
            DataSourceDefinitionDto sourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            when(sourceDefinitionDto.getScope()).thenReturn("customer");
            when(sourceDefinitionDto.getCustomId()).thenReturn(customerId);
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(sourceDefinitionDto);

            assertDoesNotThrow(() -> {
                pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response);

                // Verification results
                verify(response, times(1)).sendError(eq(404));
            });
        }

        @Test
        void testSuccess() {
            ObjectId resourceId = ObjectId.get();

            // mock data
            LinkedHashMap<String, Object> messages = new LinkedHashMap<String, Object>(){{
                put(MessageUtil.getLanguage(), new HashMap<String, String>() {{
                    put(filename, resourceId.toHexString());
                }});
            }};
            DataSourceDefinitionDto sourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            when(sourceDefinitionDto.getScope()).thenReturn("global");
            when(sourceDefinitionDto.getMessages()).thenReturn(messages);

            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(sourceDefinitionDto);

            assertDoesNotThrow(() -> {
                pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response);

                // Verification results
                verify(fileService, times(1)).viewImg(eq(resourceId), any());
                verify(response, times(0)).sendError(anyInt());
            });
        }
    }

    @Test
    public void testVersionCheck() {
        TcmService tcmService = mock(TcmService.class);
        List<DataSourceDefinitionDto> result = new ArrayList<>();
        when(dataSourceDefinitionService.findAll(any(Query.class))).thenReturn(result);
        when(tcmService.getLatestProductReleaseCreateTime()).thenReturn("2023-04-28");

        pkdSourceService.setTcmService(tcmService);

        Assertions.assertDoesNotThrow(() -> {
            List<PdkVersionCheckDto> a = pkdSourceService.versionCheck(5);
            Assertions.assertNotNull(a);
            Assertions.assertTrue(a.isEmpty());
        });

        for (int i = 0; i < 5; i++) {
            DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
            dto.setPdkId("test");
            dto.setPdkAPIBuildNumber(i);
            dto.setPdkAPIVersion("api_v" + i);
            dto.setPdkHash("123" + i);
            dto.setLastUpdAt(new Date());
            result.add(dto);
        }

        List<PdkVersionCheckDto> versionCheckResult = pkdSourceService.versionCheck(2);
        Assertions.assertEquals(1, versionCheckResult.size());
        Assertions.assertEquals("1234", versionCheckResult.get(0).getPdkHash());
        Assertions.assertEquals(DateUtil.formatDateTime(result.get(4).getLastUpdAt()), versionCheckResult.get(0).getGitBuildTime());
        Assertions.assertTrue(versionCheckResult.get(0).isLatest());

        Map<String, String> manifest = new HashMap<>();
        manifest.put("Git-Build-Time", "2023-04-25T18:05:20+0800");
        result.get(4).setManifest(manifest);

        versionCheckResult = pkdSourceService.versionCheck(2);

        Assertions.assertEquals(1, versionCheckResult.size());
        Assertions.assertEquals("1234", versionCheckResult.get(0).getPdkHash());
        Assertions.assertEquals("2023-04-25 18:05:20", versionCheckResult.get(0).getGitBuildTime());
        Assertions.assertTrue(versionCheckResult.get(0).isLatest());
    }

    @Test
    public void testUploadAndView() {

        String pdkHash = "test";
        int pdkBuildNumber = 1;
        UserDetail user = mock(UserDetail.class);
        PdkFileTypeEnum type = PdkFileTypeEnum.JAR;
        HttpServletResponse response = mock(HttpServletResponse.class);

        Assertions.assertDoesNotThrow(() -> {

            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(null);
            ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
            doNothing().when(response).sendError(captor.capture());
            pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, type, response);

            Assertions.assertEquals(404, captor.getValue());
            DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
            dto.setScope("customer");
            dto.setCustomId("customer_id");
            when(user.getCustomerId()).thenReturn("customer_id_1");
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(dto);
            Assertions.assertThrows(BizException.class, () -> {
                pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, PdkFileTypeEnum.IMAGE, response);
            });

            when(user.getCustomerId()).thenReturn("customer_id");
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(dto);
            pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, PdkFileTypeEnum.MARKDOWN, response);
            Assertions.assertTrue(captor.getAllValues().stream().anyMatch(p -> p == 404));

            dto.setMessages(new LinkedHashMap<>());
            dto.getMessages().put("zh_CN", new HashMap<String, String>(){{
                put("doc", new ObjectId().toHexString());
            }});
            dto.getMessages().put("en_US", new HashMap<String, String>(){{
                put("doc", new ObjectId().toHexString());
            }});
            dto.getMessages().put("zh_TW", new HashMap<String, String>(){{
                put("doc", new ObjectId().toHexString());
            }});

            pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, PdkFileTypeEnum.MARKDOWN, response);
            verify(fileService).viewImg(any(), any());

        });



    }
}
