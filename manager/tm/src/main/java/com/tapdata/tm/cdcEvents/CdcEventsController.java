package com.tapdata.tm.cdcEvents;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author: Zed
 * @Date: 2022/4/12
 * @Description:
 */
/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@Tag(name = "ResourceTag", description = "资源分类相关接口")
@RestController
@RequestMapping("api/CdcEvents")
public class CdcEventsController extends BaseController {

    @PostMapping("deleteAll")
    public ResponseMessage<Void> delteAll(@RequestParam("where") String whereJson) {
        return success();
    }

}
