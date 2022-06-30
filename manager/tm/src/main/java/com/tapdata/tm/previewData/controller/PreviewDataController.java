package com.tapdata.tm.previewData.controller;

import com.tapdata.tm.previewData.param.PreviewParam;
import com.tapdata.tm.previewData.service.PreviewService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Tag(name = "previewData", description = "数据预览相关接口")
@Slf4j
@RestController
@RequestMapping("/api/previewData")
public class PreviewDataController {

    @Autowired
    PreviewService previewService;

    /**
     * 因为返回的数据结构
     * {
     *     "data": {
     *         "items": [
     *             {
     *                 "_id": "620efc23dd220e04fea89cac",
     *                 "pk": 1,
     *                 "n": 1,
     *                 "title": "title1"
     *             }
     *         ],
     *         "head": [
     *             {
     *                 "text": "_id",
     *                 "value": "_id"
     *             },
     *             {
     *                 "text": "pk",
     *                 "value": "pk"
     *             },
     *             {
     *                 "text": "n",
     *                 "value": "n"
     *             },
     *             {
     *                 "text": "title",
     *                 "value": "title"
     *             }
     *         ],
     *         "total": 1
     *     },
     *     "code": "ok",
     *     "msg": "ok"
     * }
     *
     * 因返回的数据结构与其他不一样，与data同级的 多了一个head ，所以这里用map返回
     * @return
     */
    @Operation(summary = "创建一个数据库连接，然后将数据返回给前端以便预览")
    @PostMapping
    public Map preview(@RequestBody PreviewParam previewParam) {
        return previewService.preview(previewParam);
    }


}
