package com.tapdata.tm.file.service;

import com.deepoove.poi.XWPFTemplate;
import com.mongodb.Function;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Collation;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.file.entity.WaitingDeleteFile;
import com.tapdata.tm.file.repository.WaitingDeleteFileRepository;
import com.tapdata.tm.utils.GZIPUtil;
import org.apache.commons.io.IOUtils;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsResource;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/9/29 14:44
 */
public class FileServiceTest {

    @Test
    public void testFind() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        GridFSFindIterable result = mock(GridFSFindIterable.class);

        when(gridFsTemplate.find(any())).thenReturn(result);
        assertDoesNotThrow(() -> {
            GridFSFindIterable resultSet = fileService.find(Query.query(Criteria.where("filename").is("test")));
            Assertions.assertNotNull(resultSet);
        });
    }

    @Test
    public void testFindOne() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        GridFSFile gridFSFile = mock(GridFSFile.class);

        when(gridFsTemplate.findOne(any())).thenReturn(gridFSFile);
        assertDoesNotThrow(() -> {
            GridFSFile result = fileService.findOne(Query.query(Criteria.where("filename").is("test")));
            Assertions.assertNotNull(result);
        });
    }

    @Test
    public void testStoreFile() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);

        ObjectId id = new ObjectId();

        when(gridFsTemplate.store(any(), any(), any(), any())).then(ans -> {

            Assertions.assertEquals("test.png", ans.getArgument(1));

            return id;
        });
        assertDoesNotThrow(() -> {
            InputStream inputStream = new ByteArrayInputStream("".getBytes(Charset.defaultCharset()));

            ObjectId result = fileService.storeFile(inputStream, "test.png", "image/png", new HashMap<>());
            Assertions.assertNotNull(result);
            Assertions.assertEquals(id, result);
        });
    }

    @Test
    public void testReadFileById() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);

        InputStream inputStream = new ByteArrayInputStream("test".getBytes(Charset.defaultCharset()));
        GridFsResource gridFsResource = mock(GridFsResource.class);
        GridFSFile gridFSFile = mock(GridFSFile.class);

        Assertions.assertDoesNotThrow(() -> {
            when(gridFsTemplate.getResource(any(GridFSFile.class))).thenReturn(gridFsResource);
            when(gridFsResource.getInputStream()).thenReturn(inputStream);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            long length = fileService.readFileById(gridFSFile, outputStream);

            Assertions.assertEquals(4, length);
            String content = new String(outputStream.toByteArray(), Charset.defaultCharset());
            Assertions.assertEquals("test", content);

            outputStream.reset();
            when(gridFsResource.getInputStream()).thenThrow(new IOException("test"));
            long result = fileService.readFileById(gridFSFile, outputStream);
            assertEquals(0, result);

        });
    }

    @Test
    public void testReadFileByIdToResponse() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);

        InputStream inputStream = new ByteArrayInputStream("test".getBytes(Charset.defaultCharset()));
        GridFsResource gridFsResource = mock(GridFsResource.class);
        GridFSFile gridFSFile = mock(GridFSFile.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        ObjectId id = new ObjectId();

        Assertions.assertDoesNotThrow(() -> {

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(gridFsTemplate.findOne(any())).thenReturn(gridFSFile);
            when(gridFsTemplate.getResource(gridFSFile)).thenReturn(gridFsResource);
            when(gridFsResource.getInputStream()).thenReturn(inputStream);
            when(gridFsResource.getFilename()).thenReturn("test.png");
            when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(WriteListener listener) {

                }

                @Override
                public void write(int b) throws IOException {
                    outputStream.write(b);
                }
            });
            doAnswer(answer -> {
                Assertions.assertEquals("application/octet-stream", answer.getArgument(0));
                return null;
            }).when(response).setContentType(any());

            long length = fileService.readFileById(id, response);

            Assertions.assertEquals(4, length);
            Assertions.assertEquals("test", new String(outputStream.toByteArray(), Charset.defaultCharset()));

            outputStream.reset();
            when(gridFsResource.getInputStream()).thenThrow(new IOException("test"));
            long result = fileService.readFileById(gridFSFile, outputStream);
            assertEquals(0, result);
        });
    }

    @Test
    public void testDeleteFileById() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        doNothing().when(gridFsTemplate).delete(any());
        Assertions.assertDoesNotThrow(() -> {
            fileService.deleteFileById(new ObjectId());
        });
    }

    @Test
    public void testDeleteFileByPdkHash() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        doNothing().when(gridFsTemplate).delete(any());
        Assertions.assertDoesNotThrow(() -> {
            fileService.deleteFileByPdkHash("test", 2);
        });
    }

    @Test
    public void testViewImg() {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        GridFSBucket gridFSBucket = mock(GridFSBucket.class);
        FileService fileService = new FileService(gridFsTemplate);
        fileService.setGridFSBucket(gridFSBucket);
        GridFSFile gridFSFile = mock(GridFSFile.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ObjectId fileId = new ObjectId();

        Assertions.assertDoesNotThrow(() -> {

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            when(gridFsTemplate.findOne(any())).thenReturn(gridFSFile);
            when(gridFSFile.getFilename()).thenReturn("test.png");
            when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(WriteListener listener) {

                }

                @Override
                public void write(int b) throws IOException {
                    outputStream.write(b);
                }
            });

            doAnswer(answer -> {
                OutputStream output = answer.getArgument(1);
                output.write("test".getBytes(Charset.defaultCharset()));
                output.flush();
                return null;
            }).when(gridFSBucket).downloadToStream(any(ObjectId.class), any(OutputStream.class));

            fileService.viewImg(fileId, response);

            Assertions.assertEquals("test", new String(outputStream.toByteArray(), Charset.defaultCharset()));

        });

    }

    @Test
    public void testViewImg1() {

        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Assertions.assertDoesNotThrow(() -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(WriteListener listener) {

                }

                @Override
                public void write(int b) throws IOException {
                    outputStream.write(b);
                }
            });

            String fileData = "{test: 1}";
            fileService.viewImg1(fileData, response, "test.json");

            Assertions.assertEquals(fileData, new String(Objects.requireNonNull(GZIPUtil.unGzip(outputStream.toByteArray())), Charset.defaultCharset()));
        });

    }

    @Test
    public void testViewWord () {
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Assertions.assertDoesNotThrow(() -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(WriteListener listener) {

                }

                @Override
                public void write(int b) throws IOException {
                    outputStream.write(b);
                }
            });

            XWPFTemplate xwpfTemplate = mock(XWPFTemplate.class);
            doAnswer(answer -> {

                OutputStream output = answer.getArgument(0);
                output.write("test".getBytes(Charset.defaultCharset()));
                output.flush();
                return null;
            }).when(xwpfTemplate).writeAndClose(any(OutputStream.class));

            fileService.viewWord(xwpfTemplate, response, "test.xml");

            Assertions.assertEquals("test", new String(Objects.requireNonNull(outputStream.toByteArray()), Charset.defaultCharset()));

            outputStream.reset();

            fileService.viewWord(xwpfTemplate, response, null);
            Assertions.assertEquals("test", new String(Objects.requireNonNull(outputStream.toByteArray()), Charset.defaultCharset()));
        });
    }

    @Test
    public void testScheduledDeleteFiles() {

        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);

        WaitingDeleteFileRepository waitingDeleteFileRepository = mock(WaitingDeleteFileRepository.class);

        FileService fileService = new FileService(gridFsTemplate);
        fileService.setWaitingDeleteFileRepository(waitingDeleteFileRepository);

        List<ObjectId> fileIds = new ArrayList<>();
        fileIds.add(new ObjectId());
        fileIds.add(new ObjectId());
        fileIds.add(new ObjectId());

        String reason = "Upload new connector";
        String module = "connector";
        ObjectId resourceId = new ObjectId();
        UserDetail userDetail = mock(UserDetail.class);

        when(waitingDeleteFileRepository.insert(any(WaitingDeleteFile.class), any())).then(answer -> {
            Object obj = answer.getArgument(0);
            Assertions.assertTrue(obj instanceof WaitingDeleteFile);
            WaitingDeleteFile entity = (WaitingDeleteFile) obj;
            Assertions.assertEquals(fileIds.size(), entity.getFileIds().size());
            Assertions.assertEquals(reason, entity.getReason());
            Assertions.assertEquals(module, entity.getModule());
            Assertions.assertEquals(resourceId, entity.getResourceId());
            return entity;
        });

        assertDoesNotThrow(() -> {

            fileService.scheduledDeleteFiles(fileIds, reason, module, resourceId, userDetail);

        });

    }

    @Test
    public void testCleanupWaitingDeleteFiles() {
        WaitingDeleteFileRepository waitingDeleteFileRepository = mock(WaitingDeleteFileRepository.class);
        GridFsTemplate gridFsTemplate = mock(GridFsTemplate.class);
        FileService fileService = new FileService(gridFsTemplate);
        fileService.setWaitingDeleteFileRepository(waitingDeleteFileRepository);

        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        doNothing().when(gridFsTemplate).delete(captor.capture());

        List<WaitingDeleteFile> result = new ArrayList<>();
        result.add(new WaitingDeleteFile());
        result.get(0).setFileIds(Arrays.asList(new ObjectId(), new ObjectId()));
        when(waitingDeleteFileRepository.findAll(any(Query.class))).thenReturn(result);

        fileService.cleanupWaitingDeleteFiles();

        Assertions.assertEquals(2, captor.getAllValues().size());
        Assertions.assertEquals(result.get(0).getFileIds().get(0), captor.getAllValues().get(0).getQueryObject().get("_id"));
        Assertions.assertEquals(result.get(0).getFileIds().get(1), captor.getAllValues().get(1).getQueryObject().get("_id"));

    }

}
