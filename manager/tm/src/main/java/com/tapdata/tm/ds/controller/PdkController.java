package com.tapdata.tm.ds.controller;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import com.tapdata.tm.ds.vo.PdkFileTypeEnum;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author: Zed
 * @Date: 2022/2/22
 * @Description:
 */

@Tag(name = "PDK", description = "PDK相关接口")
@RestController
@RequestMapping("/api/pdk")
@Setter(onMethod_ = {@Autowired})
public class PdkController extends BaseController {
    private PkdSourceService pkdSourceService;

    @PostMapping(path = "upload/source", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<Void> uploadJar(@RequestParam("file") CommonsMultipartFile[] file, @RequestParam("source") List<String> sourceJsons, @RequestParam("latest") boolean latest) {

        List<PdkSourceDto> pdkSourceDtos = new ArrayList<>();
        for (String sourceJson : sourceJsons) {
            // if the size of sourceJsons send by client is 1, here we marshal the data in a weird way, not sure why; so
            // client add an empty string into the sourceJsons if the size is 1, which solve the problem, we should skip
            // the empty string.
            if (StringUtils.isBlank(sourceJson)) {
                continue;
            }
            PdkSourceDto pdkSourceDto = JsonUtil.parseJsonUseJackson(sourceJson, PdkSourceDto.class);
            if (Objects.isNull(pdkSourceDto.getPdkAPIBuildNumber())) {
                pdkSourceDto.setPdkAPIBuildNumber(0);
                pdkSourceDto.setPdkAPIVersion("");
            }
            pdkSourceDtos.add(pdkSourceDto);
        }

        pkdSourceService.uploadPdk(file, pdkSourceDtos, latest, getLoginUser());
        return success();
    }


    @GetMapping(value = "/jar", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void downloadJar(@RequestParam("pdkHash") String pdkHash,
                            HttpServletResponse response) throws IOException {
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "Please upgrade engine");
    }

    @GetMapping(value = "/jar/v2", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void downloadJarV2(@RequestParam("pdkHash") String pdkHash,
                            @RequestParam(value = "pdkBuildNumber", defaultValue = "0", required = false) Integer pdkBuildNumber,
                            HttpServletResponse response) {
        pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, getLoginUser(), PdkFileTypeEnum.JAR, response);
    }

    @GetMapping(value = "/icon")
    public void downloadIcon(@RequestParam("pdkHash") String pdkHash,
                             HttpServletResponse response) {
        pkdSourceService.uploadAndView(pdkHash, null, getLoginUser(),PdkFileTypeEnum.IMAGE, response);
    }

    @GetMapping(value = "/doc", produces = MediaType.TEXT_MARKDOWN_VALUE)
    public void downloadDoc(@RequestParam("pdkHash") String pdkHash,
                            HttpServletResponse response) {
        pkdSourceService.uploadAndView(pdkHash, null, getLoginUser(),PdkFileTypeEnum.MARKDOWN, response);
    }
    @GetMapping(value = "/checkMd5/v2")
    public ResponseMessage<String> checkFileMd5(@RequestParam("pdkHash") String pdkHash, @RequestParam("pdkBuildNumber") int pdkBuildNumber) {
        return success(pkdSourceService.checkJarMD5(pdkHash, pdkBuildNumber));
    }
    @GetMapping(value = "/checkMd5")
    public ResponseMessage<String> checkFileMd5(@RequestParam("pdkHash") String pdkHash, @RequestParam("fileName") String fileName) {
        return success(pkdSourceService.checkJarMD5(pdkHash, fileName));
    }

    @GetMapping(value = "/statics/{pdkHash}", produces = MediaType.TEXT_MARKDOWN_VALUE)
    public void statics(HttpServletResponse response
        , @PathVariable("pdkHash") String pdkHash
        , @RequestParam(value = "pdkBuildNumber", required = false) Integer pdkBuildNumber
        , @RequestParam("filename") String filename) throws IOException {
        String[] split = filename.split(":");
        if (split.length == 2) {
            switch (split[0]) {
                case "doc":
                    pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, getLoginUser(), response);
                    return;
                default:
                    break;
            }
        }
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found: " + filename);
    }
}
