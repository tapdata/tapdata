package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.CustomSqlService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;


@Service
@Slf4j
public class CustomSqlServiceImpl implements CustomSqlService {

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private DataSourceDefinitionService definitionService;

    @Override
    public void checkCustomSqlTask(TaskDto taskDto, UserDetail user) {
        //判断如果不是自定义sql，直接返回
        DAG dag = taskDto.getDag();
        if (dag == null) {
            return;
        }

        boolean isCustomCommand = false;
        List<Node> sources = dag.getSources();
        if (CollectionUtils.isEmpty(sources)) {
            return;
        }

        for (Node source : sources) {
            if (source instanceof TableNode) {
                boolean customCommand = ((TableNode) source).isEnableCustomCommand();
                if (customCommand) {
                    isCustomCommand = true;
                    break;
                }

            }
        }

        if (!isCustomCommand) {
            return;
        }

        //只有弱schema类型的数据源才可能作为目标

        List<Node> targets = dag.getTargets();
        if (CollectionUtils.isNotEmpty(targets)) {

            List<String> targetConIds = targets.stream().filter(s -> s instanceof TableNode).map(n ->  ((TableNode) n).getConnectionId()).collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(targetConIds)) {
                List<ObjectId> targetConObjIds = targetConIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
                Criteria criteria = Criteria.where("_id").in(targetConObjIds);
                Query query = new Query(criteria);
                query.fields().include("database_type");
                List<DataSourceConnectionDto> connectionDtos = dataSourceService.findAllDto(query, user);
                List<String> databaseTypes = connectionDtos.stream().map(DataSourceConnectionDto::getDatabase_type).collect(Collectors.toList());
                List<DataSourceDefinitionDto> definitionDtos = definitionService.getByDataSourceType(databaseTypes, user, "tags");
                if (CollectionUtils.isNotEmpty(definitionDtos)) {
                    for (DataSourceDefinitionDto definitionDto : definitionDtos) {
                        if (CollectionUtils.isNotEmpty(definitionDto.getTags())) {
                            boolean contains = definitionDto.getTags().contains("schema-free");
                            if (!contains) {
                                throw new BizException("CustomSql.TargetNotSchemaFree");
                            }

                        } else {
                            throw new BizException("CustomSql.TargetNotSchemaFree");
                        }
                    }
                }

            }
        }

        //只支持js处理器节点
        List<Node> nodes = dag.getNodes();
        for (Node node : nodes) {
            if (node instanceof ProcessorNode && !(node instanceof JsProcessorNode)) {
                throw new BizException("CustomSql.ProcessorNodeNotJs");
            }
        }
    }
}
