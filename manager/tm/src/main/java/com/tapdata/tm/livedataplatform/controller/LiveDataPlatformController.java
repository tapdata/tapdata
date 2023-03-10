package com.tapdata.tm.livedataplatform.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.livedataplatform.constant.ModeEnum;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;



@Tag(name = "LiveDataPlatform", description = "LiveDataPlatform相关接口")
@RestController
@RequestMapping({"/api/LiveDataPlatform"})
public class LiveDataPlatformController extends BaseController {

    @Autowired
    private LiveDataPlatformService liveDataPlatformService;


    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<LiveDataPlatformDto> save(@RequestBody LiveDataPlatformDto liveDataPlatformDto) {
        liveDataPlatformDto.setId(null);
        return success(liveDataPlatformService.save(liveDataPlatformDto, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param liveDataPlatformDto
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<LiveDataPlatformDto> update(@RequestBody LiveDataPlatformDto liveDataPlatformDto) {
        String mode = liveDataPlatformDto.getMode();
        if(StringUtils.isEmpty(mode) ||
                !StringUtils.equalsAny(mode, ModeEnum.INTEGRATION_PLATFORM.getValue(),
                        ModeEnum.SERVICE_PLATFORM.getValue())){
            throw new BizException("IllegalArgument","mode");
        }
        return success(liveDataPlatformService.save(liveDataPlatformDto, getLoginUser()));
    }


    /**
     * liveDataPlatform query
     */
    @GetMapping
    public ResponseMessage<Page<LiveDataPlatformDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(liveDataPlatformService.find(filter, getLoginUser()));
    }





    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<LiveDataPlatformDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(liveDataPlatformService.findOne(filter, getLoginUser()));
    }




}