package com.tapdata.tm.task.service.batchin;

import cn.hutool.core.collection.CollUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.dto.TablePathInfo;
import com.tapdata.tm.task.service.batchin.entity.AddJsNodeParam;
import com.tapdata.tm.task.service.batchin.entity.AddMergerNodeParam;
import com.tapdata.tm.task.service.batchin.entity.DoMergeParam;
import com.tapdata.tm.task.service.batchin.entity.GenericPropertiesParam;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class ParseRelMigFile implements ParseRelMig<TaskDto> {
    protected Map<String, Object> relMigInfo;
    protected String version;
    protected Map<String, Object> project;
    protected Map<String, Object> schema;
    protected List<Map<String, Object>> queries;
    protected ParseParam param;
    protected Map<String, Object> content;

    protected ParseRelMigFile(ParseParam param) {
        this.param = param;
        this.relMigInfo = Optional.ofNullable(param.getRelMigInfo()).orElse(new HashMap<>());
        this.version = String.valueOf(relMigInfo.get(KeyWords.VERSION));
        this.project = parseMap(getFromMap(relMigInfo, KeyWords.PROJECT));
        this.schema = parseMap(getFromMap(relMigInfo, KeyWords.SCHEMA));
        this.queries = Optional.ofNullable((List<Map<String, Object>>) getFromMap(relMigInfo, KeyWords.QUERIES)).orElse(new ArrayList<>());
        this.content = parseMap(getFromMap(project, KeyWords.CONTENT));
    }

    public List<String> addJsNodeIfInNeed(AddJsNodeParam param) {
        String sourceConnectionId = param.getSourceConnectionId();
        Map<String, Map<String, Map<String, Object>>> renameFields = param.getRenameFields();
        Map<String, String> sourceToJs = param.getSourceToJs();
        Map<String, Object> contentMapping = param.getContentMapping();
        Map<String, List<Map<String, Object>>> contentDeleteOperations = param.getContentDeleteOperations();
        Map<String, List<Map<String, Object>>> contentRenameOperations = param.getContentRenameOperations();
        Map<String, Object> task = param.getTask();
        List<Map<String, Object>> nodes = param.getNodes();
        List<Map<String, Object>> edges = param.getEdges();
        List<String> sourceNodes = new ArrayList<>();
        for (String key : contentMapping.keySet()) {
            Map<String, Object> node = new HashMap<>();
            Map<String, Object> contentMappingValue = parseMap(getFromMap(contentMapping, key));
            if (!contentMappingValue.get(KeyWords.COLLECTION_ID).equals(task.get(KeyWords.ID))) {
                continue;
            }

            TablePathInfo tablePathInfo = getTablePathInfo(contentMappingValue);
            String tpTable = tablePathInfo.getTable();

            node.put(KeyWords.TYPE, KeyWords.TABLE);
            node.put(KeyWords.TABLE_NAME, tpTable);
            node.put(KeyWords.NAME, tpTable);
            node.put(KeyWords.ID, key);
            node.put(KeyWords.CONNECTION_ID, sourceConnectionId);
            nodes.add(node);

            Map<String, Object> fields = parseMap(getFromMap(contentMappingValue, KeyWords.FIELDS));

            List<Map<String, Object>> renameOperations = new ArrayList<>();
            List<Map<String, Object>> deleteOperations = new ArrayList<>();
            contentDeleteOperations.put(key, deleteOperations);
            contentRenameOperations.put(key, renameOperations);

            Map<String, Map<String, Object>> tableRenameFields = new HashMap<>();
            renameFields.put(tpTable, tableRenameFields);
            addRenameOrDeleteField(fields, tableRenameFields, renameOperations, deleteOperations);

            // JS script generator
            String declareScript = KeyWords.EMPTY;
            String jsScript = jsScriptGenerator(contentMappingValue);

            String sourceId = (String) node.get(KeyWords.ID);
            //add jsNode
            if (!KeyWords.EMPTY.equals(jsScript)) {
                sourceId = addJSNode(tpTable, jsScript, declareScript, nodes, sourceId, edges);
            }

            //add rename processor node
            sourceId = addRenameNode(tpTable, renameOperations, sourceId, nodes, edges);

            //add delete processor node
            if (!deleteOperations.isEmpty()) {
                sourceId = addDeleteNode(tpTable, deleteOperations, sourceId, nodes, edges);
            }
            // save mapping
            sourceToJs.put((String) node.get(KeyWords.ID), sourceId);
            sourceNodes.add(sourceId);
        }
        return sourceNodes;
    }

    public void addRenameOrDeleteField(Map<String, Object> fields,
                                          Map<String, Map<String, Object>> tableRenameFields,
                                          List<Map<String, Object>> renameOperations,
                                          List<Map<String, Object>> deleteOperations) {
        for (String field : fields.keySet()) {
            Map<String, Object> fieldMap = parseMap(getFromMap(fields, field));
            Map<String, Object> source = parseMap(getFromMap(fieldMap, KeyWords.SOURCE));
            Map<String, Object> target = parseMap(getFromMap(fieldMap, KeyWords.TARGET));
            Map<String, Object> newName = getNewNameMap(target, source);
            tableRenameFields.put(source.get(KeyWords.NAME).toString(), newName);
            Object isPk = source.get(KeyWords.IS_PRIMARY_KEY);
            if (!Boolean.TRUE.equals(target.get(KeyWords.INCLUDED))) {
                Map<String, Object> deleteOperation = getDeleteOperation(source.get(KeyWords.NAME).toString(), isPk);
                deleteOperations.add(deleteOperation);
            } else if (!source.get(KeyWords.NAME).equals(target.get(KeyWords.NAME))) {
                Map<String, Object> renameOperation = getRenameOperation(source.get(KeyWords.NAME), target.get(KeyWords.NAME));
                renameOperations.add(renameOperation);
            }
        }
    }

    /**
     * 如果存在字段的值变换逻辑，则增加对应的js脚本，用于新增js节点
     * **/
    public String jsScriptGenerator(Map<String, Object> contentMappingValue) {
        StringBuilder script = new StringBuilder();
        Map<String, Object> calculatedFields = parseMap(getFromMap(contentMappingValue, KeyWords.CALCULATED_FIELDS));
        for (String field : calculatedFields.keySet()) {
            Map<String, Object> fieldMap = parseMap(getFromMap(calculatedFields, field));
            String newFieldName = (String) fieldMap.get(KeyWords.NAME);
            String newFieldExpression = (String) fieldMap.get(KeyWords.EXPRESSION);
            newFieldExpression = newFieldExpression.replace("columns[", "record[");
            script.append("\trecord[\"")
                    .append(newFieldName)
                    .append("\"] = ")
                    .append(newFieldExpression)
                    .append(";\n");
        }

        if (!script.toString().equals(KeyWords.EMPTY)) {
            script.insert(0, "function process(record){");
            script.append("    return record;\n}");
        }
        return script.toString();
    }

    public void objectArrayAsNormalArray(StringBuilder script, Map<String, Object> contentMappingValue, Map<String, Object> fields) {
        Map<String, Object> fieldSettingInfo = parseMap(getFromMap(contentMappingValue, KeyWords.SETTINGS));
        if (Boolean.TRUE.equals(getFromMap(fieldSettingInfo, KeyWords.PRIMITIVE))) {
            Map<String, Object> primitiveField = scanPrimitiveField(fields);
            if (!primitiveField.isEmpty()) {
                String embeddedPath = String.valueOf(getFromMap(fieldSettingInfo, KeyWords.EMBEDDED_PATH));
                Map<String, Object> source = parseMap(getFromMap(primitiveField, KeyWords.SOURCE));
                String sourceFieldName = String.valueOf(getFromMap(source, KeyWords.NAME));
                String item = "newArr_" + embeddedPath;
                script.append("\tlet ").append(item).append(" = [];\n")
                        .append("\tfor(let item of record[\"").append(embeddedPath).append("\"]) {\n")
                        .append("\t  ").append(item).append(".push(item[\"").append(sourceFieldName).append("\"]);\n")
                        .append("\t}\n")
                        .append("\trecord[\"").append(embeddedPath).append("\"] = ").append(item).append(";\n");

            }
        }
    }

    public Map<String, Object> scanPrimitiveField(Map<String, Object> fields) {
        List<Map<String, Object>> included = fields.values()
                .stream()
                .filter(o -> {
                    Map<String, Object> fieldInfo = parseMap(getFromMap(parseMap(o), KeyWords.TARGET));
                    return Boolean.TRUE.equals(fieldInfo.get(KeyWords.INCLUDED));
                }).map(this::parseMap)
                .collect(Collectors.toList());
        if (!included.isEmpty()) {
            return included.get(0);
        }
        return new HashMap<>();
    }

    public boolean addMergerNodes(AddMergerNodeParam param) {
        String rootNodeId = param.getRootNodeId();
        String mergeNodeId = param.getMergeNodeId();
        Map<String, Object> mergeNode = param.getMergeNode();
        Map<String, String> sourceToJs = param.getSourceToJs();
        Map<String, Object> contentMapping = param.getContentMapping();
        Map<String, Map<String, Map<String, Object>>> renameFields = param.getRenameFields();
        Map<String, List<Map<String, Object>>> contentDeleteOperations = param.getContentDeleteOperations();
        Map<String, List<Map<String, Object>>> contentRenameOperations = param.getContentRenameOperations();
        Map<String, Object> full = parseMap(getFromMap(schema, KeyWords.FULL));
        List<Map<String, Object>> mergeProperties = new ArrayList<>();
        Map<String, Object> relationships = content.get(KeyWords.RELATIONSHIPS) == null ? new HashMap<>() : parseMap(getFromMap(content, KeyWords.RELATIONSHIPS));
        Map<String, Object> relationshipsMapping = relationships.get(KeyWords.MAPPINGS) == null ? new HashMap<>() : parseMap(getFromMap(relationships, KeyWords.MAPPINGS));

        mergeNode.put(KeyWords.TYPE, "merge_table_processor");
        mergeNode.put(KeyWords.NAME, "merge");
        mergeNode.put(KeyWords.ID, mergeNodeId);
        mergeNode.put(CATALOG, PROCESSOR);
        mergeNode.put("mergeMode", "main_table_first");
        mergeNode.put(KeyWords.IS_TRANSFORMED, false);
        Map<String, Object> rootProperties = new HashMap<>();
        rootProperties.put(KeyWords.TARGET_PATH, KeyWords.EMPTY);
        rootProperties.put(KeyWords.ID, sourceToJs.get(rootNodeId));
        rootProperties.put(RM_ID, rootNodeId);
        rootProperties.put("mergeType", "updateOrInsert");
        TablePathInfo tablePathInfo = getTablePathInfo(contentMapping, rootNodeId);
        String tpTable = tablePathInfo.getTable();
        rootProperties.put(KeyWords.TABLE_NAME, tpTable);
        GenericPropertiesParam propertiesParam = GenericPropertiesParam.of()
                .withParent(rootProperties)
                .withContentMapping(contentMapping)
                .withRelationshipsMapping(relationshipsMapping)
                .withFull(full)
                .withSourceToJS(sourceToJs)
                .withRenameFields(renameFields)
                .withContentDeleteOperations(contentDeleteOperations).withContentRenameOperations(contentRenameOperations);
        genericProperties(propertiesParam);
        mergeProperties.add(rootProperties);
        mergeNode.put("mergeProperties", mergeProperties);
        boolean needMergeNode = true;
        List<Object> children = (List<Object>) rootProperties.get(KeyWords.CHILDREN);
        if (children == null || children.isEmpty()) {
            needMergeNode = false;
        }
        if (!needMergeNode) {
            return false;
        }
        return needMergeNode;
    }

    public void doMerge(DoMergeParam param) {
        String mergeNodeId = param.getMergeNodeId();
        List<Map<String, Object>> nodes = param.getNodes();
        Map<String, Object> contentMapping = param.getContentMapping();
        Map<String, Object> mergeNode = param.getMergeNode();
        List<String> sourceNodes = param.getSourceNodes();
        List<Map<String, Object>> edges = param.getEdges();
        Map<String, Map<String, Map<String, Object>>> renameFields = param.getRenameFields();
        Map<String, List<Map<String, Object>>> contentRenameOperations = param.getContentRenameOperations();
        Map<String, List<Map<String, Object>>> contentDeleteOperations = param.getContentDeleteOperations();
        nodes.add(mergeNode);
        for (String sourceId : sourceNodes) {
            Map<String, Object> edge = new HashMap<>();
            edge.put(KeyWords.SOURCE, sourceId);
            edge.put(KeyWords.TARGET, mergeNodeId);
            edges.add(edge);
        }
        contentDeleteOperations.forEach((k, v) -> {
            Map<String, Object> contentMappingValue = parseMap(getFromMap(contentMapping, k));
            TablePathInfo pathInfo = getTablePathInfo(contentMappingValue);
            String finalTable = pathInfo.getTable();
            v.removeIf(delOp -> {
                boolean flag = Boolean.TRUE.equals(delOp.get(KeyWords.IS_PK));
                if (flag) {
                    Map<String, Map<String, Object>> map = renameFields.get(finalTable);
                    String name = delOp.get(KeyWords.FIELD).toString();
                    Map<String, Object> renameOperation = getRenameOperation(name, map.get(name).get(KeyWords.TARGET).toString());
                    contentRenameOperations.get(k).add(renameOperation);
                }
                return flag;
            });
        });
    }

    public List<Map<String, Object>> getTaskInfoList() {
        List<Map<String, Object>> tpTasks = new ArrayList<>();
        Map<String, Object> contentCollections = parseMap(getFromMap(content, KeyWords.COLLECTIONS));
        for (String key : contentCollections.keySet()) {
            Map<String, Object> task = new HashMap<>();
            Map<String, Object> map = parseMap(getFromMap(contentCollections, key));
            Object name = map.get(KeyWords.NAME);
            task.put(KeyWords.ID, key);
            task.put(KeyWords.NAME, name);
            task.put(KeyWords.TARGET_MODEL_NAME, name);
            tpTasks.add(task);
        }
        return tpTasks;
    }

    public Map<String, String> doParse(String sourceConnectionId, String targetConnectionId, UserDetail user) {
        Map<String, String> sourceToJs = new HashMap<>();
        Map<String, String> parsedTpTasks = new HashMap<>();
        Map<String, Map<String, Map<String, Object>>> renameFields = new HashMap<>();
        replaceRmProjectId();

        List<Map<String, Object>> tpTasks = getTaskInfoList();
        for (Map<String, Object> task : tpTasks) {
            task.put("editVersion", System.currentTimeMillis());
            task.put("syncType", KeyWords.SYNC);
            task.put(KeyWords.TYPE, "initial_sync+cdc");
            task.put("mappingTemplate", KeyWords.SYNC);
            task.put("status", "edit");
            task.put("user_id", user.getUserId());
            task.put("customId", user.getCustomerId());
            task.put("createUser", user.getUsername());

            String targetModelName = (String) task.get("targetModelName");

            Map<String, Object> dag = new HashMap<>();
            List<Map<String, Object>> nodes = new ArrayList<>();
            List<Map<String, Object>> edges = new ArrayList<>();

            Map<String, Object> contentMapping = parseMap(getFromMap(content, KeyWords.MAPPINGS));

            // 把源节点都加进去, 这里如果有一些 字段改名, 或者新字段生成的操作, 增加一个 JS 处理器
            Map<String, List<Map<String, Object>>> contentDeleteOperations = new HashMap<>();
            Map<String, List<Map<String, Object>>> contentRenameOperations = new HashMap<>();
            List<String> sourceNodes = addJsNodeIfInNeed(AddJsNodeParam.of().withSourceConnectionId(sourceConnectionId)
                    .withRenameFields(renameFields)
                    .withSourceToJs(sourceToJs)
                    .withContentMapping(contentMapping).withContentDeleteOperations(contentDeleteOperations)
                    .withContentRenameOperations(contentRenameOperations)
                    .withTask(task)
                    .withEdges(edges)
                    .withNodes(nodes));
            String rootTableName = KeyWords.EMPTY;
            Map<String, Object> rootContentMappingValue = new HashMap<>();
            List<String> keyAfterFilterByNewDocument = contentMapping.keySet().stream()
                    .filter(key -> keyAfterByNewDocumentFilter(key, contentMapping, task))
                    .collect(Collectors.toList());
            String rootNodeId = KeyWords.EMPTY;
            if(!keyAfterFilterByNewDocument.isEmpty()) {
                rootNodeId = keyAfterFilterByNewDocument.get(0);
                rootContentMappingValue = parseMap(getFromMap(contentMapping, rootNodeId));
                TablePathInfo table = getTablePathInfo(rootContentMappingValue);
                rootTableName = table.getTable();
            }
            if (StringUtils.isBlank(rootNodeId)) {
                continue;
            }

            // 增加主从合并节点
            String mergeNodeId = UUID.randomUUID().toString().toLowerCase();
            Map<String, Object> mergeNode = new HashMap<>();
            boolean needMergeNode = addMergerNodes(AddMergerNodeParam.of()
                    .withRootNodeId(rootNodeId)
                    .withMergeNodeId(mergeNodeId)
                    .withMergeNode(mergeNode)
                    .withSourceToJs(sourceToJs)
                    .withContentMapping(contentMapping)
                    .withRenameFields(renameFields)
                    .withContentDeleteOperations(contentDeleteOperations)
                    .withContentRenameOperations(contentRenameOperations));
            if (needMergeNode) {
                doMerge(DoMergeParam.of().withMergeNodeId(mergeNodeId)
                        .withNodes(nodes)
                        .withContentMapping(contentMapping)
                        .withMergeNode(mergeNode)
                        .withSourceNodes(sourceNodes)
                        .withEdges(edges)
                        .withRenameFields(renameFields)
                        .withContentRenameOperations(contentRenameOperations)
                        .withContentDeleteOperations(contentDeleteOperations));
            }

            // 把目标节点加进去
            Map<String, Object> targetNode = new HashMap<>();
            targetNode.put(KeyWords.TYPE, KeyWords.TABLE);
            targetNode.put("existDataProcessMode", "keepData");
            targetNode.put(KeyWords.NAME, targetModelName);
            targetNode.put(KeyWords.ID, task.get(KeyWords.ID));
            targetNode.put(KeyWords.CONNECTION_ID, targetConnectionId);
            targetNode.put(KeyWords.TABLE_NAME, targetModelName);
            List<String> updateConditionFields = new ArrayList<>();
            Map<String, Map<String, Object>> rootRenameFields = renameFields.get(rootTableName);
            Map<String, Object> rootFields = parseMap(getFromMap(rootContentMappingValue, KeyWords.FIELDS));
            for (String field : rootFields.keySet()) {
                Map<String, Object> fieldMap = parseMap(getFromMap(rootFields, field));
                Map<String, Object> source = parseMap(getFromMap(fieldMap, KeyWords.SOURCE));
                if (Boolean.TRUE.equals(source.get(KeyWords.IS_PRIMARY_KEY))) {
                    updateConditionFields.add(rootRenameFields.get(field).get(KeyWords.TARGET).toString());
                }
            }

            targetNode.put("updateConditionFields", updateConditionFields);
            nodes.add(targetNode);
            Map<String, Object> edge = new HashMap<>();
            edge.put(KeyWords.SOURCE, needMergeNode ? mergeNodeId : sourceNodes.get(0));
            edge.put(KeyWords.TARGET, targetNode.get(KeyWords.ID));
            edges.add(edge);

            dag.put("nodes", nodes);
            dag.put("edges", edges);
            task.put("dag", dag);
            parsedTpTasks.put((String) task.get(KeyWords.ID), JsonUtil.toJson(task));
        }
        return parsedTpTasks;
    }
    
    protected boolean keyAfterByNewDocumentFilter(String key, Map<String, Object> contentMapping, Map<String, Object> task) {
        Map<String, Object> contentMappingValue = parseMap(getFromMap(contentMapping, key));
        if (!contentMappingValue.get(KeyWords.COLLECTION_ID).equals(task.get(KeyWords.ID))) {
            return false;
        }
        Map<String, Object> setting = parseMap(getFromMap(contentMappingValue, KeyWords.SETTINGS));
        if (Objects.isNull(setting)) {
            return false;
        }
        Object type = setting.get(KeyWords.TYPE);
        if (Objects.isNull(type)) {
            return false;
        }
        return KeyWords.NEW_DOCUMENT.equals(type);
    }

    public void resetMap(Map<String, Object> map, Map<String, String> globalIdMap) {
        ArrayList<String> keys = new ArrayList<>(map.keySet());
        for (String key : keys) {
            Map<String, Object> collection = parseMap(getFromMap(map, key));
            String replaceKey = replaceId(key, globalIdMap);
            map.remove(key);
            map.put(replaceKey, collection);
        }
    }

    public void replaceRmProjectId() {
        Map<String, String> globalIdMap = new HashMap<>();
        Map<String, Object> contentCollections = parseMap(getFromMap(content, KeyWords.COLLECTIONS));
        resetMap(contentCollections, globalIdMap);

        Map<String, Object> contentMapping = parseMap(getFromMap(content, KeyWords.MAPPINGS));
        resetMap(contentMapping, globalIdMap);

        Set<String> contentMappingKeys = new HashSet<>(contentMapping.keySet());
        for (String key : contentMappingKeys) {
            Map<String, Object> mapping = parseMap(getFromMap(contentMapping, key));
            mapping.put(KeyWords.COLLECTION_ID, replaceId((String) mapping.get(KeyWords.COLLECTION_ID), globalIdMap));
        }
        replaceRelationShipsKey(globalIdMap, content);
    }

    public String addJSNode(String tpTable, String script, String declareScript, List<Map<String, Object>> nodes, String sourceId, List<Map<String, Object>> edges) {
        String jsId = UUID.randomUUID().toString().toLowerCase();
        Map<String, Object> jsNode = new HashMap<>();
        jsNode.put(KeyWords.TYPE, "js_processor");
        jsNode.put(KeyWords.NAME, tpTable);
        jsNode.put(KeyWords.ID, jsId);
        jsNode.put("jsType", 1);
        jsNode.put(PROCESSOR_THREAD_NUM, 1);
        jsNode.put(CATALOG, PROCESSOR);
        jsNode.put(ELEMENT_TYPE, KeyWords.NODE);
        jsNode.put("script", script);
        jsNode.put("declareScript", declareScript);
        nodes.add(jsNode);
        Map<String, Object> edge = new HashMap<>();
        edge.put(KeyWords.SOURCE, sourceId);
        edge.put(KeyWords.TARGET, jsId);
        edges.add(edge);
        return jsId;
    }

    public String addRenameNode(String tpTable, List<Map<String, Object>> renameOperations, String sourceId, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> renameNode = new HashMap<>();
        String renameId = UUID.randomUUID().toString().toLowerCase();
        renameNode.put(KeyWords.ID, renameId);
        renameNode.put(CATALOG, PROCESSOR);
        renameNode.put(ELEMENT_TYPE, KeyWords.NODE);
        renameNode.put("fieldsNameTransform", KeyWords.EMPTY);
        renameNode.put(KeyWords.IS_TRANSFORMED, false);
        renameNode.put(KeyWords.NAME, "Rename " + tpTable);
        renameNode.put(PROCESSOR_THREAD_NUM, 1);
        renameNode.put(KeyWords.TYPE, "field_rename_processor");
        nodes.add(renameNode);
        renameNode.put(KeyWords.OPERATIONS, renameOperations);
        Map<String, Object> edge = new HashMap<>();
        edge.put(KeyWords.SOURCE, sourceId);
        edge.put(KeyWords.TARGET, renameId);
        edges.add(edge);
        sourceId = renameId;
        return sourceId;
    }

    public String addDeleteNode(String tpTable, List<Map<String, Object>> deleteOperations, String sourceId, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> deleteNode = new HashMap<>();
        String deleteId = UUID.randomUUID().toString().toLowerCase();
        deleteNode.put(KeyWords.ID, deleteId);
        deleteNode.put(CATALOG, PROCESSOR);
        deleteNode.put("deleteAllFields", false);
        deleteNode.put(ELEMENT_TYPE, KeyWords.NODE);
        deleteNode.put(KeyWords.NAME, "Delete " + tpTable);
        deleteNode.put(KeyWords.TYPE, "field_add_del_processor");
        deleteNode.put(PROCESSOR_THREAD_NUM, 1);
        deleteNode.put(KeyWords.OPERATIONS, deleteOperations);
        nodes.add(deleteNode);
        Map<String, Object> edge = new HashMap<>();
        edge.put(KeyWords.SOURCE, sourceId);
        edge.put(KeyWords.TARGET, deleteId);
        edges.add(edge);
        sourceId = deleteId;
        return sourceId;
    }

    public Map<String, Object> getDeleteOperation(Object deleteFieldName, Object isPrimaryKey) {
        Map<String, Object> deleteOperation = new HashMap<>();
        deleteOperation.put(KeyWords.ID, UUID.randomUUID().toString().toLowerCase());
        deleteOperation.put(KeyWords.FIELD, deleteFieldName);
        deleteOperation.put(KeyWords.OP, "REMOVE");
        deleteOperation.put(KeyWords.OPERAND, "true");
        deleteOperation.put("label", deleteFieldName);
        deleteOperation.put(KeyWords.IS_PK, isPrimaryKey);
        return deleteOperation;
    }

    public Map<String, Object> getRenameOperation(Object source, Object target) {
        Map<String, Object> fieldRenameOperation = new HashMap<>();
        fieldRenameOperation.put(KeyWords.ID, UUID.randomUUID().toString().toLowerCase());
        fieldRenameOperation.put(KeyWords.FIELD, source);
        fieldRenameOperation.put(KeyWords.OP, "RENAME");
        fieldRenameOperation.put(KeyWords.OPERAND, target);
        return fieldRenameOperation;
    }

    public Map<String, Object> getNewNameMap(Map<String, Object> target, Map<String, Object> source) {
        Map<String, Object> newName = new HashMap<>();
        newName.put(KeyWords.TARGET, target.get(KeyWords.NAME).toString());
        newName.put(KeyWords.IS_PRIMARY_KEY, source.get(KeyWords.IS_PRIMARY_KEY));
        return newName;
    }

    public TablePathInfo getTablePathInfo(Map<String, Object> contentMapping) {
        // spilt by dot, and get last one
        String table = String.valueOf(contentMapping.get(KeyWords.TABLE));
        String[] tablePathAfterSplit = table.split("\\.");
        TablePathInfo tablePathInfo = new TablePathInfo();
        switch (tablePathAfterSplit.length) {
            case 1:
                tablePathInfo.setTable(tablePathAfterSplit[0]);
                break;
            case 2:
                tablePathInfo.setSchema(tablePathAfterSplit[0]);
                tablePathInfo.setTable(tablePathAfterSplit[1]);
                break;
            case 3:
                tablePathInfo.setDatabase(tablePathAfterSplit[0]);
                tablePathInfo.setSchema(tablePathAfterSplit[1]);
                tablePathInfo.setTable(tablePathAfterSplit[2]);
                break;
            default:
        }
        return tablePathInfo;
    }

    public TablePathInfo getTablePathInfo(Map<String, Object> contentMapping, String childId) {
        Map<String, Object> childMap = parseMap(getFromMap(contentMapping, childId));
        return getTablePathInfo(childMap);
    }

    public void scanAllFieldKeys(Map<String, Map<String, Object>> currentRenameFields,
                                    Map<String, Object> childNode,
                                    Map<String, Object> fields) {
        List<String> arrayKeys = new ArrayList<>();
        for (String field : fields.keySet()) {
            Map<String, Object> fieldMap = parseMap(getFromMap(fields, field));
            Map<String, Object> source = parseMap(getFromMap(fieldMap, KeyWords.SOURCE));
            if (Boolean.TRUE.equals(source.get(KeyWords.IS_PRIMARY_KEY))) {
                Object target = currentRenameFields.get(field).get(KeyWords.TARGET);
                if (Objects.isNull(target)) {
                    continue;
                }
                arrayKeys.add(String.valueOf(target));
            }
        }
        childNode.put(KeyWords.ARRAY_KEYS, arrayKeys);
    }

    public void genericProperties(GenericPropertiesParam param) {
        Map<String, Object> parent = param.getParent();
        Map<String, Object> relationshipsMapping = param.getRelationshipsMapping();
        String parentId = (String) parent.get(RM_ID);
        List<String> children = (List<String>) (parseMap(getFromMap(relationshipsMapping, parentId))).get(KeyWords.CHILDREN);
        if (!CollUtil.isEmpty(children)) {
            List<Map<String, Object>> childrenNode = new ArrayList<>();
            children.stream().forEach(child -> childrenNode.add(scanOneChildren(param, child)));
            parent.put(KeyWords.CHILDREN, childrenNode);
        }
    }
    
    public Map<String, Object> scanOneChildren(GenericPropertiesParam param, String child) {
        Map<String, Object> parent = param.getParent();
        String parentId = (String) parent.get(RM_ID);
        Map<String, Object> contentMapping = param.getContentMapping();
        Map<String, Object> full = param.getFull();
        Map<String, String> sourceToJS = param.getSourceToJS();
        Map<String, Object> relationshipsMapping = param.getRelationshipsMapping();
        Map<String, Map<String, Map<String, Object>>> renameFields = param.getRenameFields();
        Map<String, List<Map<String, Object>>> contentDeleteOperations = param.getContentDeleteOperations();
        Map<String, List<Map<String, Object>>> contentRenameOperations = param.getContentRenameOperations();
        Map<String, Object> childNode = new HashMap<>();
        Map<String, Object> map = parseMap(getFromMap(contentMapping, child));
        Map<String, Object> setting = parseMap(getFromMap(map, KeyWords.SETTINGS));

        TablePathInfo tableInfo = getTablePathInfo(contentMapping, child);
        Map<String, Object> tables = getTableSchema(full, tableInfo);

        String tpTable = tableInfo.getTable();
        List<Map<String, String>> joinKeys = new ArrayList<>();
        Map<String, Object> currentTable = parseMap(getFromMap(tables, tpTable));
        Map<String, Object> currentColumns = parseMap(getFromMap(currentTable, KeyWords.COLUMNS));
        Map<String, Map<String, Object>> currentRenameFields = renameFields.get(tpTable);
        Map<String, Object> parentTable = parseMap(getFromMap(tables, String.valueOf(parent.get(KeyWords.TABLE_NAME))));
        Map<String, Object> parentColumns = parseMap(getFromMap(parentTable, KeyWords.COLUMNS));
        Map<String, Map<String, Object>> parentRenameFields = renameFields.get(String.valueOf(parent.get(KeyWords.TABLE_NAME)));

        String parentTargetPath = (String) Optional.ofNullable(parent.get(KeyWords.TARGET_PATH)).orElse(KeyWords.EMPTY);
        String mergeType = KeyWords.UPDATE_WRITE;
        String targetPath = KeyWords.EMPTY;
        String type = String.valueOf(setting.get(KeyWords.TYPE)).toUpperCase();
        switch (type) {
            case KeyWords.NEW_DOCUMENT:
                targetPath = parentTargetPath;
                break;
            case KeyWords.EMBEDDED_DOCUMENT:
                targetPath = getEmbeddedDocumentPath(parentTargetPath, setting);
                break;
            case KeyWords.EMBEDDED_DOCUMENT_ARRAY:
                final String embeddedPath = String.valueOf(setting.get(KeyWords.EMBEDDED_PATH));
                targetPath = KeyWords.EMPTY.equals(parentTargetPath) ?
                        String.format(KeyWords.FROMAT, parentTargetPath, embeddedPath)
                        : embeddedPath;
                mergeType = KeyWords.UPDATE_INTO_ARRAY;
                Map<String, Object> fields = parseMap(getFromMap(map, KeyWords.FIELDS));
                scanAllFieldKeys(currentRenameFields, childNode, fields);
                break;
            default:
                // doNothing
        }


        childNode.put(KeyWords.TARGET_PATH, targetPath);
        childNode.put(KeyWords.MERGE_TYPE, mergeType);

        childNode.put(KeyWords.TABLE_NAME, tpTable);
        childNode.put(KeyWords.CHILDREN, new ArrayList<>());
        childNode.put(KeyWords.ID, sourceToJS.get(child));
        childNode.put(RM_ID, child);

        // 由于使用外键做关联, 所以似乎 RM 只能合并来自一个源的数据, 所以 tables 表结构使用其中一个就可以

        Map<String, Map<String, String>> sourceJoinKeyMapping = new HashMap<>();
        Map<String, Map<String, String>> targetJoinKeyMapping = new HashMap<>();
        for (String columnKey : currentColumns.keySet()) {
            Map<String, Object> column = parseMap(getFromMap(currentColumns, columnKey));
            Optional.ofNullable(getFromMap(column, KeyWords.FOREIGN_KEY)).ifPresent(fk -> {
                Map<String, Object> foreignKey = parseMap(fk);
                if (String.valueOf(foreignKey.get(KeyWords.TABLE)).equals(parent.get(KeyWords.TABLE_NAME))) {
                    Map<String, String> joinKey = new HashMap<>();
                    String sourceJoinKey = currentRenameFields.get(columnKey).get(KeyWords.TARGET).toString();
                    Map<String, String> newFieldMap = new HashMap<>();
                    newFieldMap.put(KeyWords.SOURCE, columnKey);
                    newFieldMap.put(KeyWords.TARGET, sourceJoinKey);
                    sourceJoinKeyMapping.put(sourceJoinKey, newFieldMap);
                    joinKey.put(KeyWords.SOURCE, sourceJoinKey);
                    Object columnObject = getFromMap(foreignKey, KeyWords.COLUMN);
                    Map<String, Object> parentRenameField = parseMap(parentRenameFields.get(String.valueOf(columnObject)));
                    String targetJoinKey = (String)parentRenameField.get(KeyWords.TARGET);
                    HashMap<String, String> targetNewFieldMap = new HashMap<>();
                    targetNewFieldMap.put(KeyWords.SOURCE, (String) foreignKey.get(KeyWords.COLUMN));
                    targetNewFieldMap.put(KeyWords.TARGET, targetJoinKey);
                    if (KeyWords.EMPTY.equals(parent.get(KeyWords.TARGET_PATH))) {
                        joinKey.put(KeyWords.TARGET, targetJoinKey);
                    } else {
                        joinKey.put(KeyWords.TARGET, String.format(KeyWords.FROMAT, parent.get(KeyWords.TARGET_PATH), targetJoinKey));
                    }
                    targetJoinKeyMapping.put(targetJoinKey, targetNewFieldMap);
                    joinKeys.add(joinKey);
                }
            });
        }

        parentColumnsFindJoinKeys(parent, renameFields, parentColumns, tpTable, joinKeys, sourceJoinKeyMapping, targetJoinKeyMapping);
        childNode.put(KeyWords.JOIN_KEYS, joinKeys);
        joinKeys.forEach(joinKeyMap -> {
            String sourceJoinKey = joinKeyMap.get(KeyWords.SOURCE);
            addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations, contentRenameOperations, child, sourceJoinKeyMapping, sourceJoinKey);
            String targetJoinKey = joinKeyMap.get(KeyWords.TARGET);
            addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations, contentRenameOperations, parentId, targetJoinKeyMapping, targetJoinKey);
        });
        GenericPropertiesParam propertiesParam = GenericPropertiesParam.of()
                .withParent(childNode)
                .withContentMapping(contentMapping)
                .withRelationshipsMapping(relationshipsMapping)
                .withFull(full)
                .withSourceToJS(sourceToJS)
                .withRenameFields(renameFields)
                .withContentDeleteOperations(contentDeleteOperations).withContentRenameOperations(contentRenameOperations);
        genericProperties(propertiesParam);
        return childNode;
    }

    public String replaceId(String id, Map<String, String> globalIdMap) {
        if (globalIdMap.containsKey(id)) {
            return globalIdMap.get(id);
        }
        String newId = UUID.randomUUID().toString();
        globalIdMap.put(id, newId);
        return newId;
    }

    public void replaceRelationShipsKey(Map<String, String> globalIdMap, Map<String, Object> content) {
        Map<String, Object> relationships = content.get(KeyWords.RELATIONSHIPS) == null ? new HashMap<>() : parseMap(getFromMap(content, KeyWords.RELATIONSHIPS));
        Map<String, Object> relationshipsCollection = relationships.get(KeyWords.COLLECTIONS) == null ? new HashMap<>() : parseMap(getFromMap(relationships, KeyWords.COLLECTIONS));
        Set<String> relationshipsCollectionKeys = new HashSet<>(relationshipsCollection.keySet());
        Map<String, Object> relationshipsMapping = relationships.get(KeyWords.MAPPINGS) == null ? new HashMap<>() : parseMap(getFromMap(relationships, KeyWords.MAPPINGS));
        for (String key : relationshipsCollectionKeys) {
            Map<String, Object> collection = parseMap(getFromMap(relationshipsCollection, key));
            String replaceKey = replaceId(key, globalIdMap);
            relationshipsCollection.remove(key);
            relationshipsCollection.put(replaceKey, collection);
        }
        for (String key : relationshipsCollection.keySet()) {
            Map<String, Object> collection = parseMap(getFromMap(relationshipsCollection, key));
            List<String> oldMappingIds = (List<String>) collection.get(KeyWords.MAPPINGS);
            List<String> newMappingIds = new ArrayList<>();
            for (String oldMappingId : oldMappingIds) {
                newMappingIds.add(replaceId(oldMappingId, globalIdMap));
            }
            collection.put(KeyWords.MAPPINGS, newMappingIds);
        }

        Set<String> relationshipsMappingKeys = new HashSet<>(relationshipsMapping.keySet());
        for (String key : relationshipsMappingKeys) {
            Map<String, Object> mapping = parseMap(getFromMap(relationshipsMapping, key));
            String replaceKey = replaceId(key, globalIdMap);
            relationshipsMapping.remove(key);
            relationshipsMapping.put(replaceKey, mapping);
        }

        for (String key : relationshipsMapping.keySet()) {
            Map<String, Object> mapping = parseMap(getFromMap(relationshipsMapping, key));
            List<String> oldChildren = (List<String>) mapping.get(KeyWords.CHILDREN);
            List<String> newChildren = new ArrayList<>();
            for (String oldChild : oldChildren) {
                newChildren.add(replaceId(oldChild, globalIdMap));
            }
            mapping.put(KeyWords.CHILDREN, newChildren);
        }
    }

    public Map<String, Object> getTableSchema(Map<String, Object> full, TablePathInfo tableInfo) {
        Map<String, Object> databasesLayer = parseMap(getFromMap(full, KeyWords.DATABASE));
        Map<String, Object> currentDatabaseSchema = parseMap(getFromMap(databasesLayer, tableInfo.getDatabase()));
        Map<String, Object> schemasLayer = parseMap(getFromMap(currentDatabaseSchema, KeyWords.SCHEMA));
        Map<String, Object> currentSchema = parseMap(getFromMap(schemasLayer, tableInfo.getSchema()));
        return parseMap(getFromMap(currentSchema, KeyWords.TABLES));
    }

    public String getEmbeddedDocumentPath(String parentTargetPath, Map<String, Object> setting) {
        String targetPath;
        if (KeyWords.EMPTY.equals(parentTargetPath)) {
            targetPath = String.valueOf(setting.get(KeyWords.EMBEDDED_PATH));
        } else {
            Object embeddedPath = setting.get(KeyWords.EMBEDDED_PATH);
            if (null == embeddedPath || StringUtils.isBlank(String.valueOf(embeddedPath))) {
                targetPath = parentTargetPath;
            } else {
                targetPath = String.format(KeyWords.FROMAT, parentTargetPath, embeddedPath);
            }
        }
        return targetPath;
    }

    public void parentColumnsFindJoinKeys(Map<String, Object> parent, Map<String, Map<String, Map<String, Object>>> renameFields, Map<String, Object> parentColumns, String tpTable, List<Map<String, String>> joinKeys, Map<String, Map<String, String>> souceJoinKeyMapping, Map<String, Map<String, String>> targetJoinKeyMapping) {
        Object tableName = parent.get(KeyWords.TABLE_NAME);
        Map<String, Map<String, Object>> parentRenameFields = renameFields.get(String.valueOf(tableName));
        for (String columnKey : parentColumns.keySet()) {
            Map<String, Object> column = parseMap(getFromMap(parentColumns, columnKey));
            Map<String, Object> foreignKey = parseMap(getFromMap(column, KeyWords.FOREIGN_KEY));
            if (foreignKey == null) {
                continue;
            }
            if (null != tpTable && tpTable.equals(foreignKey.get(KeyWords.TABLE))) {
                Map<String, String> joinKey = new HashMap<>();
                Object columnObj = foreignKey.get(KeyWords.COLUMN);
                String sourceJoinKey = renameFields.get(tpTable).get(String.valueOf(columnObj)).get(KeyWords.TARGET).toString();
                Map<String, String> sourceNewFieldMap = new HashMap<>();
                sourceNewFieldMap.put(KeyWords.SOURCE, (String) foreignKey.get(KeyWords.COLUMN));
                sourceNewFieldMap.put(KeyWords.TARGET, sourceJoinKey);
                souceJoinKeyMapping.put(sourceJoinKey, sourceNewFieldMap);
                joinKey.put(KeyWords.SOURCE, sourceJoinKey);
                Map<String, String> targetNewFieldMap = new HashMap<>();
                String targetJoinKey = parentRenameFields.get(columnKey).get(KeyWords.TARGET).toString();
                targetNewFieldMap.put(KeyWords.SOURCE, columnKey);
                targetNewFieldMap.put(KeyWords.TARGET, targetJoinKey);
                String targetPath = parent.get(KeyWords.TARGET_PATH).toString();
                if (!StringUtils.isBlank(targetPath)) {
                    targetJoinKey = String.format(KeyWords.FROMAT, targetPath, targetJoinKey);
                }
                joinKey.put(KeyWords.TARGET, targetJoinKey);
                targetJoinKeyMapping.put(targetJoinKey, targetNewFieldMap);
                joinKeys.add(joinKey);
            }
        }
    }

    public void addRenameOpIfDeleteOpHasJoinKey(Map<String, List<Map<String, Object>>> contentDeleteOperations, Map<String, List<Map<String, Object>>> contentRenameOperations, String tableId, Map<String, Map<String, String>> joinKeyMapping, String joinKey) {
        List<Map<String, Object>> childDeleteOperations = contentDeleteOperations.get(tableId);
        boolean removeJoinKeyFlag = removeDeleteOperation(childDeleteOperations, joinKeyMapping, joinKey);
        if (removeJoinKeyFlag) {
            List<Map<String, Object>> childRenameOperations = contentRenameOperations.get(tableId);
            Map<String, Object> renameOperation = getRenameOperation(joinKeyMapping.get(joinKey).get(KeyWords.SOURCE), joinKeyMapping.get(joinKey).get(KeyWords.TARGET));
            childRenameOperations.add(renameOperation);
        }
    }

    public boolean removeDeleteOperation(List<Map<String, Object>> deleteOperations, Map<String, Map<String, String>> joinKeyMapping, String joinKey) {
        return deleteOperations.removeIf(delOperations -> {
            String deleteField = (String) delOperations.get(KeyWords.FIELD);
            Map<String, String> joinKeyInfo = joinKeyMapping.get(joinKey);
            if (null != joinKeyInfo) {
                String originalField = joinKeyInfo.get(KeyWords.SOURCE);
                return null != deleteField && deleteField.equals(originalField);
            }
            return false;
        });
    }

    public Object getFromMap(Map<String, Object> map, String key) {
        if (null == map || !map.containsKey(key)) {
            return null;
        }
        return map.get(key);
    }

    public Map<String, Object> parseMap(Object object) {
        if (object instanceof Map) {
            return (Map<String, Object>) object;
        }
        return new HashMap<>();
    }
}
