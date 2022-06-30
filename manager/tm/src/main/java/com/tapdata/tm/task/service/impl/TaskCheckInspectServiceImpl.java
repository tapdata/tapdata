package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskCheckInspectService;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liujiaxin
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class TaskCheckInspectServiceImpl implements TaskCheckInspectService {

    private DataSourceService dataSourceService;
    private DataSourceDefinitionService dataSourceDefinitionService;

    @Override
    public TaskDto getInspectFlagDefaultFlag(TaskDto taskDto, UserDetail userDetail) {
        if (Objects.isNull(taskDto.getDag())) {
            return taskDto;
        }

        List<String> connectIdList = taskDto.getDag().getNodes().stream()
                .filter(node -> node instanceof DatabaseNode)
                .map(node -> ((DatabaseNode) node).getConnectionId())
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(connectIdList)) {
            return taskDto;
        }

        List<DataSourceConnectionDto> connectionDtoList = dataSourceService.findAllByIds(connectIdList);
        if (CollectionUtils.isEmpty(connectionDtoList)) {
            return taskDto;
        }

        List<String> pdkHashList = connectionDtoList.stream()
                .map(DataSourceConnectionDto::getPdkHash)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(pdkHashList)) {
            return taskDto;
        }

        boolean has = dataSourceDefinitionService.checkHasSomeCapability(pdkHashList, userDetail, CapabilityEnum.QUERY_BY_ADVANCE_FILTER_FUNCTION);
        if (has) {
            taskDto.setCanOpenInspect(true);
        }

        return taskDto;
    }
}
