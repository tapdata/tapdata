package com.tapdata.tm.uploadlog.controller;


import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.uploadlog.dto.UploadLogDto;
import com.tapdata.tm.uploadlog.service.UploadLogService;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/uploadLog")
public class UploadLogController extends BaseController {

    final UploadLogService uploadLogService;

    public UploadLogController(UploadLogService uploadLogService) {
                this.uploadLogService = uploadLogService;
    }

    @Operation(summary = "upload agent log")
    @PostMapping("/upload")
    public ResponseMessage<String> upload(@RequestBody UploadLogDto uploadLogDto) {
        return success(uploadLogService.upload(uploadLogDto,getLoginUser()));
    }

}
