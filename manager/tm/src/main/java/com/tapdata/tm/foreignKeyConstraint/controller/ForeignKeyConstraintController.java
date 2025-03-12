package com.tapdata.tm.foreignKeyConstraint.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.foreignKeyConstraint.dto.ForeignKeyConstraintDto;
import com.tapdata.tm.foreignKeyConstraint.service.ForeignKeyConstraintService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

@Tag(name = "ForeignKeyConstraint", description = "ForeignKeyConstraint相关接口")
@RestController
@Slf4j
@RequestMapping("/api/foreignKeyConstraint")
public class ForeignKeyConstraintController extends BaseController {
    @Autowired
    private ForeignKeyConstraintService foreignKeyConstraintService;

    @Operation(summary = "给engin调用，更新foreignKeyConstraint集合")
    @PostMapping("/upsertWithWhere")
    public ResponseMessage<ForeignKeyConstraintDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ForeignKeyConstraintDto foreignKeyConstraintDto) {
        log.info("engin upsertWithWhere whereJson:{}, foreignKeyConstraint:{} ", whereJson, JSON.toJSONString(foreignKeyConstraintDto));
        Where where = parseWhere(whereJson);
        foreignKeyConstraintService.upsertByWhere(where, foreignKeyConstraintDto, getLoginUser());
        return success(foreignKeyConstraintDto);
    }

    @Operation(summary = "导出重建外键约束sql")
    @GetMapping("/load")
    public void batchLoadTasks(@RequestParam("taskId") String taskId, HttpServletResponse response) {
        foreignKeyConstraintService.loadSqlFile(taskId, response);
    }


}
