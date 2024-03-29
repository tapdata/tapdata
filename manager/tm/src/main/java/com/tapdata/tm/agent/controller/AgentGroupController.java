package com.tapdata.tm.agent.controller;

import com.tapdata.tm.agent.dto.AgentGroupDto;
import com.tapdata.tm.agent.dto.AgentRemoveFromGroupDto;
import com.tapdata.tm.agent.dto.AgentToGroupDto;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.dto.GroupUsedDto;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.agent.util.AgentGroupTag;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author XiaoGavin
 * @Date: 2021/10/15
 * @Description: Agent分组
 */
@Tag(name = "AgentGroupController", description = "Agent分组相关接口")
@RestController
@Slf4j
@RequestMapping(value = {"/api/agent-group"})
public class AgentGroupController extends BaseController {
    @Autowired
    AgentGroupService agentGroupService;

    /**
     * 分页返回 agent group with agents
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all agents which grouped of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<AgentGroupDto>> groupAllAgent(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson,
            @RequestParam(value = "containWorker", required = false) Boolean containWorker) {
        return success(agentGroupService.groupAllAgent(parseFilter(filterJson), containWorker, getLoginUser()));
    }

    /**
     * create a agent group
     * @param groupDto
     * @return
     */
    @Operation(summary = "Find all agent of the model matched by filter from the data source")
    @PostMapping("/create-group")
    public ResponseMessage<AgentGroupDto> createAgentGroup(@RequestBody GroupDto groupDto) {
        return success(agentGroupService.createGroup(groupDto, getLoginUser()));
    }

    /**
     * add a agent to agent group
     * @param agentDto
     * @return
     */
    @Operation(summary = "Find all agent of the model matched by filter from the data source")
    @PostMapping("/add-agent")
    public ResponseMessage<AgentGroupDto> addAgentToGroup(@RequestBody AgentToGroupDto agentDto) {
        return success(agentGroupService.addAgentToGroup(agentDto, getLoginUser()));
    }


    /**
     * remove a agent from agent group
     * @param removeDto
     * @return
     */
    @Operation(summary = "Find all agent of the model matched by filter from the data source")
    @PostMapping("/remove-agent")
    public ResponseMessage<AgentGroupDto> removeAgentFromGroup(@RequestBody AgentRemoveFromGroupDto removeDto) {
        return success(agentGroupService.removeAgentFromGroup(removeDto, getLoginUser()));
    }


    /**
     * delete a agent group
     * @param groupId
     * @return
     */
    @Operation(summary = "Find all agent of the model matched by filter from the data source")
    @DeleteMapping("{groupId}")
    public ResponseMessage<GroupUsedDto> deleteGroup(@PathVariable("groupId") String groupId) {
        return success(agentGroupService.deleteGroup(groupId, getLoginUser()));
    }


    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<AgentGroupDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
            filter.setWhere(Where.where(AgentGroupTag.TAG_DELETE, false));
        }
        return success(agentGroupService.findAgentGroupInfo(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source
     *
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update-group")
    public ResponseMessage<AgentGroupDto> updateByWhere(@RequestBody GroupDto dto) {
        return success(agentGroupService.updateBaseInfo(dto, getLoginUser()));
    }

}
