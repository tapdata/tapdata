package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateDateProcessorNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.DateNodeService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.utils.DataMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
public class DateNodeServiceImpl implements DateNodeService {

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private DataSourceDefinitionService definitionService;
    @Override
    public void checkTaskDateNode(TaskDto taskDto, UserDetail userDetail) {
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            return;
        }

        DAG dag = taskDto.getDag();
        if (dag == null) {
            return;
        }

        List<Node> nodes = dag.getNodes();
        if (nodes.stream().noneMatch(s -> s instanceof MigrateDateProcessorNode)) {
            return;
        }

        List<Node> sources = dag.getSources();
        DatabaseNode sourceNode = (DatabaseNode) sources.get(0);


        Set<String> dataTypes = new HashSet<>();
        for (Node node : nodes) {
            if (node instanceof MigrateDateProcessorNode) {
                List<String> dataTypes1 = ((MigrateDateProcessorNode) node).getDataTypes();
                if (CollectionUtils.isNotEmpty(dataTypes1)) {
                    dataTypes.addAll(dataTypes1);
                }
            }
        }

        String connectionId = sourceNode.getConnectionId();

        Field field = new Field();
        field.put("database_type", true);
        DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId), field, userDetail);
        if (dataSourceConnectionDto == null) {
            return;
        }
        DataSourceDefinitionDto definitionDto = definitionService.getByDataSourceType(dataSourceConnectionDto.getDatabase_type(), userDetail);
        if (definitionDto ==  null) {
            return;
        }

        String expression = definitionDto.getExpression();
        Map map = JsonUtil.parseJson(expression, Map.class);
        for (String dataType : dataTypes) {
            Object exprResult = map.get(dataType);
            if (exprResult == null) {
                throw new BizException("Task.DateProcessConfigInvalid");
            }
        }

    }
}
