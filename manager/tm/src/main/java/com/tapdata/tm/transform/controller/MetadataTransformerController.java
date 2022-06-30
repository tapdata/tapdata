package com.tapdata.tm.transform.controller;

import com.tapdata.tm.base.controller.BaseController;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;



/**
 * @Date: 2022/03/04
 * @Description:
 */
@Tag(name = "MetadataTransformer", description = "MetadataTransformer相关接口")
@RestController
@RequestMapping("/api/metadataTransformers")
public class MetadataTransformerController extends BaseController {

}