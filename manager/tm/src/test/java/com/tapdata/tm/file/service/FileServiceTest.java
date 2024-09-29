package com.tapdata.tm.file.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.file.entity.WaitingDeleteFile;
import com.tapdata.tm.file.repository.WaitingDeleteFileRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/9/29 14:44
 */
public class FileServiceTest {


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

}
