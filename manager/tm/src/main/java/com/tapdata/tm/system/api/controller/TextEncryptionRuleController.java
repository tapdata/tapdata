package com.tapdata.tm.system.api.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.service.TextEncryptionRuleService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 14:38 Create
 * @description 文本加密规则配置相关接口
 */
@Tag(name = "TextEncryptionRule", description = "文本加密规则配置相关接口")
@RestController
@RequestMapping("/api/encryption")
public class TextEncryptionRuleController extends BaseController {
    @Autowired
    protected TextEncryptionRuleService textEncryptionRuleService;

    @Operation(summary = "获取文本加密规则配置")
    @GetMapping("/get/{id}")
    public ResponseMessage<List<TextEncryptionRuleDto>> get(@PathVariable String id) {
        return success(textEncryptionRuleService.getById(id));
    }

    @Operation(summary = "获取文本加密规则配置")
    @GetMapping("/list")
    public ResponseMessage<Page<TextEncryptionRuleDto>> list(@Parameter(in = ParameterIn.QUERY, description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`).")
                                                             @RequestParam(value = "filter", required = false) String filterJson) {
        final Filter filter = Optional.ofNullable(parseFilter(filterJson)).orElse(new Filter());
        return success(textEncryptionRuleService.page(filter));
    }

    @Operation(summary = "创建文本加密规则配置")
    @PostMapping("/create")
    public ResponseMessage<Boolean> create(@RequestBody TextEncryptionRuleDto dto) {
        return success(textEncryptionRuleService.create(dto, getLoginUser()));
    }

    @Operation(summary = "更新文本加密规则配置")
    @PatchMapping
    public ResponseMessage<Boolean> update(@RequestBody TextEncryptionRuleDto dto) {
        return success(textEncryptionRuleService.update(dto, getLoginUser()));
    }

    @Operation(summary = "删除文本加密规则配置")
    @DeleteMapping("/{ids}")
    public ResponseMessage<Boolean> delete(@PathVariable String ids) {
        return success(textEncryptionRuleService.batchDelete(ids, getLoginUser()));
    }
}
