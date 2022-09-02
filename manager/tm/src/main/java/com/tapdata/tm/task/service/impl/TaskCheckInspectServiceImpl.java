package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskCheckInspectService;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

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
        if (taskDto.isAutoInspect()) {
            boolean canOpenInspect = Optional.ofNullable(taskDto.getDag()).map(dag -> {
                Set<String> dataSets = new HashSet<>();
                for (Node<?> node : dag.getNodes()) {
                    if (node instanceof DatabaseNode) {
                        dataSets.add(((DatabaseNode) node).getConnectionId());
                    }
                }
                if (!dataSets.isEmpty()) {
                    List<DataSourceConnectionDto> connections = dataSourceService.findAllByIds(new ArrayList<>(dataSets));
                    if (null != connections && !connections.isEmpty()) {
                        dataSets.clear();
                        for (DataSourceConnectionDto connDto : connections) {
                            dataSets.add(connDto.getPdkHash());
                        }
                        return dataSourceDefinitionService.checkHasSomeCapability(dataSets, userDetail, CapabilityEnum.QUERY_BY_ADVANCE_FILTER_FUNCTION);
                    }
                }
                return false;
            }).orElse(false);

            taskDto.setCanOpenInspect(canOpenInspect);//是否支持校验
        }

        return taskDto;
    }
}
