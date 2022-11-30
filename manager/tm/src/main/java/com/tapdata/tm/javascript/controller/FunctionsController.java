package com.tapdata.tm.javascript.controller;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.javascript.dto.FunctionsDto;
import com.tapdata.tm.javascript.param.SaveFunctionParam;
import com.tapdata.tm.javascript.service.FunctionService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * function  控制器
 */
@Tag(name = "Javascript", description = "java function 相关接口")
@Slf4j
//@RestController
@RequestMapping("/api/Javascript_functions")
public class FunctionsController extends BaseController {
    @Autowired
    FunctionService javascriptService;

    /**
     * 添加数据源连接
     *
     * @return
     */
    @Operation(summary = "添加 Javascript_functions")
    @PostMapping
    public ResponseMessage save(@RequestBody SaveFunctionParam saveFunctionParam) {
        return success(javascriptService.save(saveFunctionParam, getLoginUser()));
    }

    /**
     * 修改函数
     *
     * @param updateDto 修改后的名称
     * @return
     */
    @Operation(summary = "修改函数连接")
    @PatchMapping
    public ResponseMessage<FunctionsDto> update(@RequestBody FunctionsDto updateDto) {
        return success(javascriptService.update(updateDto, getLoginUser()));
    }

    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "根据条件查询数据源连接列表")
    @GetMapping
    public ResponseMessage find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);

        if (filter == null) {
            filter = new Filter();
        }

        Page<FunctionsDto> dataSourceConnectionDtoPage = javascriptService.findPage(filter, getLoginUser());

        return success(dataSourceConnectionDtoPage);
    }


    /**
     * 暂时没有使用
     *
     * @param dataSource
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<FunctionsDto> put(@RequestBody FunctionsDto dataSource) {
        return success(javascriptService.replaceOrInsert(dataSource, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
  /*  @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = javascriptService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }*/


    /**
     * 暂时没有使用
     *
     * @param javascriptDto
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<FunctionsDto> updateById(@PathVariable("id") String id, @RequestBody(required = false) FunctionsDto javascriptDto) {
        if (javascriptDto == null) {
            javascriptDto = new FunctionsDto();
        }
        javascriptDto.setId(MongoUtils.toObjectId(id));
        return success(javascriptService.update(javascriptDto, getLoginUser()));
    }

    /**
     * @param id
     * @return
     */
    @Operation(summary = "根据id查询函数")
    @GetMapping("{id}")
    public ResponseMessage<FunctionsDto> findById(@PathVariable("id") String id) {
        return success(javascriptService.findById(MongoUtils.toObjectId(id)));
    }



    /**
     * 删除 函数
     *
     * @param id
     * @return
     */
    @Operation(summary = "删除 函数")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        javascriptService.deleteLogicsById((id));
        return success();
    }



    /**
     * 根据条件查询数据源连接列表  不再使用
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "根据条件查询数据源连接列表")
    @GetMapping("count")
    public ResponseMessage<Map<String, Long>> count(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "where", required = false) String whereJson) {
        Where where = parseWhere(whereJson);
        long count = javascriptService.count(where, getLoginUser());


        HashMap<String, Long> returnMap = new HashMap<>();
        returnMap.put("count", count);

        return success(returnMap);
    }


    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<FunctionsDto> findOne( @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(javascriptService.findOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage updateByWhere(@RequestParam("where") String whereJson, @RequestBody String dto) {
        Where where = parseWhere(whereJson);
        FunctionsDto javascriptDto = JsonUtil.parseJson(dto, FunctionsDto.class);
        javascriptService.updateByWhere(where, javascriptDto, getLoginUser());
        return success(javascriptDto);
    }


    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<FunctionsDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody FunctionsDto dataSource) {
        Where where = parseWhere(whereJson);
        return success(javascriptService.upsertByWhere(where, dataSource, getLoginUser()));
    }


}
