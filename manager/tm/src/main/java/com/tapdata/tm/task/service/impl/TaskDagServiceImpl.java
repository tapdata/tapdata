package com.tapdata.tm.task.service.impl;/**
 * Created by jiuyetx on 2022/8/30 11:16
 */

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.bean.JsScriptInfoVo;
import com.tapdata.tm.task.service.TaskDagService;
import com.tapdata.tm.utils.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * @author jiuyetx
 * @date 2022/8/30
 */
@Service
@Slf4j
public class TaskDagServiceImpl implements TaskDagService {
    @Override
    public int calculationDagHash(TaskDto taskDto) {
        if (true) {
            return new Random().nextInt();
        }
        DAG dag = taskDto.getDag();

        LinkedList<DatabaseNode> sourceNode = dag.getSourceNode();

        List<String> tableNames = sourceNode.getFirst().getTableNames();

        LinkedHashSet<TableRenameTableInfo> tableRenameTableInfos = new LinkedHashSet<>();
        LinkedList<TableFieldInfo> fieldsMapping = new LinkedList<>();
        LinkedList<JsScriptInfoVo> scripts = new LinkedList<>();
        for (Node node : dag.getNodes()) {
            if (node instanceof TableRenameProcessNode) {
                TableRenameProcessNode tableNode = (TableRenameProcessNode) node;
                if (CollectionUtils.isNotEmpty(tableNode.getTableNames())) {
                    tableRenameTableInfos.addAll(tableNode.getTableNames());
                }
            } else if (node instanceof MigrateFieldRenameProcessorNode) {
                MigrateFieldRenameProcessorNode fieldNode = (MigrateFieldRenameProcessorNode) node;
                if (CollectionUtils.isNotEmpty(fieldNode.getFieldsMapping())) {
                    fieldsMapping.addAll(fieldNode.getFieldsMapping());
                }
            } else if (node instanceof MigrateJsProcessorNode) {
                MigrateJsProcessorNode jsNode = (MigrateJsProcessorNode) node;
                scripts.add(new JsScriptInfoVo(jsNode.getScript(), jsNode.getDeclareScript()));
            } else if (node instanceof DatabaseNode || node instanceof VirtualTargetNode) {
                log.info("not need do some");
            } else {
                throw new BizException("new migrate node need calculate dag hash");
            }
        }

        List<SyncObjects> syncObjects = Lists.newArrayList();
        LinkedList<DatabaseNode> targetNode = dag.getTargetNode();
        if (CollectionUtils.isNotEmpty(targetNode)
                && Objects.nonNull(targetNode.getLast())
                && CollectionUtils.isNotEmpty(targetNode.getLast().getSyncObjects())) {
            syncObjects.addAll(targetNode.getLast().getSyncObjects());
        }

        LinkedList<Object> data = new LinkedList<>();
        data.add(tableNames);
        data.add(tableRenameTableInfos);
        data.add(fieldsMapping);
        data.add(scripts);
        data.add(syncObjects);

        String jsonString = JsonUtil.toJsonUseJackson(data);

        int hash = 0;
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        try {
            JsonNode jsonNode = mapper.readTree(factory.createParser(jsonString));
            hash = jsonNode.hashCode();
        } catch (IOException e) {
            log.error("Error generating hash for jsonString: {} {}", jsonString, e.getMessage());
        }

        return hash;
    }
}
