package com.tapdata.tm.ds.service.impl;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.file.service.FileService;
import lombok.SneakyThrows;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
            CommonsMultipartFile[] files = new CommonsMultipartFile[1];
            CommonsMultipartFile file = mock(CommonsMultipartFile.class);
            files[0] = file;
            List<PdkSourceDto > pdkSourceDtos = new ArrayList<>();
            PdkSourceDto pdkSourceDto = mock(PdkSourceDto.class);
            pdkSourceDtos.add(pdkSourceDto);
            boolean latest = false;
            UserDetail user = mock(UserDetail.class);
            FileItem item = mock(FileItem.class);
            when(pdkSourceDto.getVersion()).thenReturn("1.0-SNAPSHOT");
            when(file.getOriginalFilename()).thenReturn("a.jar");
            when(file.getFileItem()).thenReturn(item);
            when(file.getName()).thenReturn("a.jar");
            InputStream ins = mock(InputStream.class);
            when(file.getInputStream()).thenReturn(ins);
            when(ins.read(any())).thenReturn(-1);
            when(fileService.storeFile(any(),anyString(),any(),anyMap())).thenReturn(mock(ObjectId.class));
            pkdSourceService.uploadPdk(files,pdkSourceDtos,latest,user);
            verify(fileService).storeFile(any(),anyString(),any(),anyMap());
            FileUtils.deleteQuietly(new File("a.jar"));
        }
    }
    @Nested
    class inputStreamToFileTest{
        @Test
        @SneakyThrows
        void testInputStreamToFile(){
            InputStream ins = mock(InputStream.class);
            when(ins.read(any())).thenReturn(-1);
            File file = new File("a.jar");
            file.createNewFile();
            assertDoesNotThrow(()->PkdSourceService.inputStreamToFile(ins,file));
            file.delete();
        }
    }
    @Nested
    class checkJarMD5{
        @Test
        void testCheckJarMD5(){
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
    }
}
