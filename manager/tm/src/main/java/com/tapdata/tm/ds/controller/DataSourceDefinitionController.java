package com.tapdata.tm.ds.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.dto.DataSourceDefinitionUpdateDto;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@Tag(name = "DataSourceDefinition", description = "数据源定义相关接口")
@RestController
@CrossOrigin
@RequestMapping("" +
        "api/DatabaseTypes")
public class DataSourceDefinitionController extends BaseController {

    @Autowired
    private DataSourceDefinitionService dataSourceDefinitionService;

    /**
     * 添加数据源定义
     * @param definition
     * @return
     */
    @PostMapping
    @Operation(summary = "添加数据源定义")
    public ResponseMessage<DataSourceDefinitionDto> add(@RequestBody DataSourceDefinitionDto definition) {
        return success(dataSourceDefinitionService.save(definition, getLoginUser()));
    }

    /**
     *  修改数据源定义信息
     * @param definitionUpdateDto 修改后的名称
     * @return
     */
    @PatchMapping
    @Operation(summary = "修改数据源定义信息")
    public ResponseMessage<DataSourceDefinitionDto> update(@RequestBody DataSourceDefinitionUpdateDto definitionUpdateDto) {
        return success(dataSourceDefinitionService.update(getLoginUser(), definitionUpdateDto));
    }

    /**
     * 根据id删除数据源定义
     * @param id
     * @return
     */
    @Operation(summary = "根据id删除数据源定义")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        dataSourceDefinitionService.delete(getLoginUser(), id);
        return success();
    }

    /**
     *  根据id查询数据源定义，需要判断用户id
     * @param id
     * @return
     */
    @Operation(summary = "根据id查询数据源定义")
    @GetMapping("{id}")
    public ResponseMessage<DataSourceDefinitionDto> getById(@PathVariable("id") String id) {
        return success(dataSourceDefinitionService.findById(toObjectId(id), getLoginUser()));
    }

    /**
     *  根据id查询数据源定义，需要判断用户id
     * @param pdkHash
     * @return
     */
    @Operation(summary = "根据id查询数据源定义")
    @GetMapping("/pdkHash/{pdkHash}")
    public ResponseMessage<DataSourceDefinitionDto> getByPdkId(@PathVariable("pdkHash") String pdkHash) {
        return success(dataSourceDefinitionService.findByPdkHash(pdkHash, getLoginUser()));
    }

    /**
     * 根据条件查询数据源类型列表
     * @param filterJson
     * @return
     */
    @Operation(summary = "根据条件查询数据源类型列表")
    @GetMapping
    public ResponseMessage<List<DataSourceTypeDto>> dataSourceTypes(@RequestParam(value = "filter", required = false) String filterJson) {

        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(dataSourceDefinitionService.dataSourceTypes(null, filter));
    }

    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<DataSourceDefinitionDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody DataSourceDefinitionDto dataSourceTypeDto) {
        Where where = parseWhere(whereJson);
        if (where.get("type") != null) {
            where.put("dataSourceType", where.get("type"));
            where.remove("type");
        }
        return success(dataSourceDefinitionService.upsertByWhere(where, dataSourceTypeDto, getLoginUser()));
    }


    /**
     * 根据条件查询数据源类型列表
     * @param filterJson
     * @return
     */
    @Operation(summary = "根据条件查询数据源类型列表")
    @GetMapping("/getDatabases")
    public ResponseMessage<List<DataSourceTypeDto>> dataSourceTypesV2(@RequestParam(value = "filter", required = false) String filterJson) {

        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(dataSourceDefinitionService.dataSourceTypesV2(getLoginUser(), filter));
    }

}
