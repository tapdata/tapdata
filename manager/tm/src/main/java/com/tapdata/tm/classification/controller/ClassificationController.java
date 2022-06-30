package com.tapdata.tm.classification.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.classification.dto.ClassificationDto;
import com.tapdata.tm.classification.service.ClassificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@Tag(name = "ResourceTag", description = "资源分类相关接口")
@RestController
@RequestMapping("api/v1/classification")
public class ClassificationController extends BaseController {

    @Autowired
    private ClassificationService classificationService;

    /**
     * 添加资源分类
     * @param tag 数据源连接实体
     * @return
     */
    @Operation(summary = "添加资源分类")
    @PostMapping
    public ResponseMessage<ClassificationDto> add(@RequestBody ClassificationDto tag) {
        return success(classificationService.save(tag, getLoginUser()));
    }

    /**
     *  修改资源分类名称
     * @param newName
     * @return
     */
    @Operation(summary = "修改资源分类名称")
    @PatchMapping("{id}")
    public ResponseMessage<ClassificationDto> rename(@PathVariable("id") String id, @RequestParam("newName") String newName) {
        return success(classificationService.rename(getLoginUser(), id, newName));
    }

    /**
     * 根据id删除数据源分类
     * @param id
     * @return
     */
    @Operation(summary = "根据id删除数据源分类")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        classificationService.delete(getLoginUser(), id);
        return success();
    }

    /**
     *  根据id查询数据源分类，需要判断用户id
     * @param id
     * @return
     */
    @Operation(summary = "根据id查询数据源分类")
    @GetMapping("{id}")
    public ResponseMessage<ClassificationDto> getById(@PathVariable("id") String id) {
        return success(classificationService.findById(new ObjectId(id), getLoginUser()));
    }

    /**
     * 根据条件查询数资源分类
     * @param filterJson
     * @return
     */
    @Operation(summary = "根据条件查询数资源分类")
    @GetMapping
    public ResponseMessage<Page<ClassificationDto>> list(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(classificationService.find(filter, getLoginUser()));
    }
}
