package com.tapdata.tm.group.service.transfer;

import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.config.security.UserDetail;
import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

@Data
public class GroupImportRequest {
    private Object resource;
    private UserDetail user;
    private ImportModeEnum importMode;

    public GroupImportRequest(Object resource, UserDetail user, ImportModeEnum importMode) {
        this.resource = resource;
        this.user = user;
        this.importMode = importMode;
    }
}
