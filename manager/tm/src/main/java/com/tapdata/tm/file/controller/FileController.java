package com.tapdata.tm.file.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2022/3/16
 * @Description:
 */
@RestController
@RequestMapping("api/file")
public class FileController extends BaseController {
    @Autowired
    private FileService fileService1;

    @GetMapping("json")
    public void jsonLoad(HttpServletResponse response) {
        fileService1.viewImg1(null, response, null);
    }

    @PostMapping(path = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<Map<String, String>> upload(@RequestParam(value = "file") MultipartFile file) {
        String upload = null;
        try {
            ObjectId objectId = fileService1.storeFile(file.getInputStream(), file.getOriginalFilename(), null, new HashMap<>());
            upload = objectId.toHexString();
        } catch (IOException e) {
            throw new BizException("SystemError");
        }

        Map<String, String> returnMap = new HashMap<>();
        returnMap.put("id", upload);
        return success(returnMap);
    }


    @GetMapping("/{fileId}")
    public ResponseMessage<Void> upload(@PathVariable(value = "fileId") String fileId, HttpServletResponse response) {
        fileService1.viewImg(MongoUtils.toObjectId(fileId), response);
        return success();
    }
}
