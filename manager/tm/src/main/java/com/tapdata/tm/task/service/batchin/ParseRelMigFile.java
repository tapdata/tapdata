package com.tapdata.tm.task.service.batchin;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.dto.RelMigBaseDto;
import com.tapdata.tm.task.service.batchin.dto.TablePathInfo;
import com.tapdata.tm.task.service.batchin.entity.AddJsNodeParam;
import com.tapdata.tm.task.service.batchin.entity.AddMergerNodeParam;
import com.tapdata.tm.task.service.batchin.entity.DoMergeParam;
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

public abstract class ParseRelMigFile implements ParseRelMig<TaskDto, RelMigBaseDto> {
    Map<String, Object> relMigInfo;
    protected String version;
    protected Map<String, Object> project;
    protected Map<String, Object> schema;
    protected List<Map<String, Object>> queries;
    protected ParseParam<RelMigBaseDto> param;
    protected Map<String, Object> content;

    public ParseRelMigFile(ParseParam<RelMigBaseDto> param) {
        this.param = param;
        this.relMigInfo = param.getRelMigInfo();
        this.version = String.valueOf(relMigInfo.get(KeyWords.VERSION));
        this.project = parseMap(getFromMap(relMigInfo, KeyWords.PROJECT));
        this.schema = parseMap(getFromMap(relMigInfo, KeyWords.SCHEMA));
        this.queries = Optional.ofNullable((List<Map<String, Object>>) getFromMap(relMigInfo, KeyWords.QUERIES)).orElse(new ArrayList<>());
        this.content = parseMap(getFromMap(project, KeyWords.CONTENT));
    }

    public List<String> addJsNode(AddJsNodeParam param) {
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
            if (!contentMappingValue.get("collectionId").equals(task.get("id"))) {
                continue;
            }

            TablePathInfo tablePathInfo = getTablePathInfo(contentMappingValue);
            String tpTable = tablePathInfo.getTable();

            node.put("type", "table");
            node.put("tableName", tpTable);
            node.put("name", tpTable);
            node.put("id", key);
            node.put("connectionId", sourceConnectionId);
            nodes.add(node);

            Map<String, Object> fields = parseMap(getFromMap(contentMappingValue, "fields"));

            List<Map<String, Object>> renameOperations = new ArrayList<>();
            List<Map<String, Object>> deleteOperations = new ArrayList<>();
            contentDeleteOperations.put(key, deleteOperations);
            contentRenameOperations.put(key, renameOperations);

            Map<String, Map<String, Object>> tableRenameFields = new HashMap<>();
            renameFields.put(tpTable, tableRenameFields);
            addRenameOrDeleteField(fields, tableRenameFields, renameOperations, deleteOperations);

            // JS script generator
            String declareScript = "";
            String jsScript = jsScriptGenerator(contentMappingValue);

            String sourceId = (String) node.get("id");
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
            sourceToJs.put((String) node.get("id"), sourceId);
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
            Map<String, Object> source = parseMap(getFromMap(fieldMap, "source"));
            Map<String, Object> target = parseMap(getFromMap(fieldMap, "target"));
            Map<String, Object> newName = getNewNameMap(target, source);
            tableRenameFields.put(source.get("name").toString(), newName);
            Object isPk = source.get("isPrimaryKey");
            if (!(Boolean) target.get("included")) {
                Map<String, Object> deleteOperation = getDeleteOperation(source.get("name").toString(), isPk);
                deleteOperations.add(deleteOperation);
                continue;
            }

            if (source.get("name").equals(target.get("name"))) {
                continue;
            }

            Map<String, Object> renameOperation = getRenameOperation(source.get("name"), target.get("name"));
            renameOperations.add(renameOperation);
        }
    }

    /**
     * 如果存在字段的值变换逻辑，则增加对应的js脚本，用于新增js节点
     * **/
    public String jsScriptGenerator(Map<String, Object> contentMappingValue) {
        StringBuilder script = new StringBuilder();
        Map<String, Object> calculatedFields = parseMap(getFromMap(contentMappingValue, "calculatedFields"));
        for (String field : calculatedFields.keySet()) {
            Map<String, Object> fieldMap = parseMap(getFromMap(calculatedFields, field));
            String newFieldName = (String) fieldMap.get("name");
            String newFieldExpression = (String) fieldMap.get("expression");
            newFieldExpression = newFieldExpression.replace("columns[", "record[");
            script.append("\trecord[\"")
                    .append(newFieldName)
                    .append("\"] = ")
                    .append(newFieldExpression)
                    .append(";\n");
        }

        if (!script.toString().equals("")) {
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
        Map<String, Object> full = parseMap(getFromMap(schema, "full"));
        List<Map<String, Object>> mergeProperties = new ArrayList<>();
        Map<String, Object> relationships = content.get("relationships") == null ? new HashMap<>() : parseMap(getFromMap(content, "relationships"));
        Map<String, Object> relationshipsMapping = relationships.get("mappings") == null ? new HashMap<>() : parseMap(getFromMap(relationships, "mappings"));

        mergeNode.put("type", "merge_table_processor");
        mergeNode.put("name", "merge");
        mergeNode.put("id", mergeNodeId);
        mergeNode.put(CATALOG, PROCESSOR);
        mergeNode.put("mergeMode", "main_table_first");
        mergeNode.put("isTransformed", false);
        Map<String, Object> rootProperties = new HashMap<>();
        rootProperties.put("targetPath", "");
        rootProperties.put("id", sourceToJs.get(rootNodeId));
        rootProperties.put(RM_ID_KEY, rootNodeId);
        rootProperties.put("mergeType", "updateOrInsert");
        TablePathInfo tablePathInfo = getTablePathInfo(contentMapping, rootNodeId);
        String tpTable = tablePathInfo.getTable();
        rootProperties.put("tableName", tpTable);
        genProperties(rootProperties, contentMapping, relationshipsMapping, full, sourceToJs, renameFields, contentDeleteOperations, contentRenameOperations);
        mergeProperties.add(rootProperties);
        mergeNode.put("mergeProperties", mergeProperties);
        boolean needMergeNode = true;
        List<Object> children = (List<Object>) rootProperties.get("children");
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
        Map<String, Object> schema = parseMap(getFromMap(relMigInfo, KeyWords.SCHEMA));
        nodes.add(mergeNode);
        for (String sourceId : sourceNodes) {
            Map<String, Object> edge = new HashMap<>();
            edge.put("source", sourceId);
            edge.put("target", mergeNodeId);
            edges.add(edge);
        }
        contentDeleteOperations.forEach((k, v) -> {
            Map<String, Object> contentMappingValue = parseMap(getFromMap(contentMapping, k));
            TablePathInfo pathInfo = getTablePathInfo(contentMappingValue);
            String finalTable = pathInfo.getTable();
            v.removeIf((delOp) -> {
                boolean flag = Boolean.TRUE.equals(delOp.get("isPk"));
                if (flag) {
                    Map<String, Map<String, Object>> map = renameFields.get(finalTable);
                    String name = delOp.get("field").toString();
                    Map<String, Object> renameOperation = getRenameOperation(name, map.get(name).get("target").toString());
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
            task.put("syncType", "sync");
            task.put("type", "initial_sync+cdc");
            task.put("mappingTemplate", "sync");
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
            List<String> sourceNodes = addJsNode(AddJsNodeParam.of().withSourceConnectionId(sourceConnectionId)
                    .withRenameFields(renameFields)
                    .withSourceToJs(sourceToJs)
                    .withContentMapping(contentMapping).withContentDeleteOperations(contentDeleteOperations)
                    .withContentRenameOperations(contentRenameOperations)
                    .withTask(task)
                    .withEdges(edges)
                    .withNodes(nodes));

            String rootNodeId = "";
            String rootTableName = "";
            Map<String, Object> rootContentMappingValue = new HashMap<>();
            for (String key : contentMapping.keySet()) {
                Map<String, Object> contentMappingValue = parseMap(getFromMap(contentMapping, key));
                if (!contentMappingValue.get("collectionId").equals(task.get("id"))) {
                    continue;
                }
                Map<String, Object> setting = parseMap(getFromMap(contentMappingValue, "settings"));
                if (setting == null) {
                    continue;
                }
                String type = (String) setting.get("type");
                if (type == null) {
                    continue;
                }
                if (type.equals("NEW_DOCUMENT")) {
                    rootNodeId = key;
                    rootContentMappingValue = contentMappingValue;
                    TablePathInfo table = getTablePathInfo(rootContentMappingValue);
                    rootTableName = table.getTable();
                    break;
                }
            }

            if (rootNodeId == null || rootNodeId.equals("")) {
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
            targetNode.put("type", "table");
            targetNode.put("existDataProcessMode", "keepData");
            targetNode.put("name", targetModelName);
            targetNode.put("id", task.get("id"));
            targetNode.put("connectionId", targetConnectionId);
            targetNode.put("tableName", targetModelName);
            List<String> updateConditionFields = new ArrayList<>();
            Map<String, Map<String, Object>> rootRenameFields = renameFields.get(rootTableName);
            Map<String, Object> rootFields = parseMap(getFromMap(rootContentMappingValue, "fields"));
            for (String field : rootFields.keySet()) {
                Map<String, Object> fieldMap = parseMap(getFromMap(rootFields, field));
                Map<String, Object> source = parseMap(getFromMap(fieldMap, "source"));
                Boolean isPrimaryKey = (Boolean) source.get("isPrimaryKey");
                if (isPrimaryKey != null && isPrimaryKey) {
                    updateConditionFields.add(rootRenameFields.get(field).get("target").toString());
                }
            }

            targetNode.put("updateConditionFields", updateConditionFields);
            nodes.add(targetNode);
            Map<String, Object> edge = new HashMap<>();
            if (needMergeNode) {
                edge.put("source", mergeNodeId);
            } else {
                edge.put("source", sourceNodes.get(0));
            }
            edge.put("target", targetNode.get("id"));
            edges.add(edge);

            dag.put("nodes", nodes);
            dag.put("edges", edges);
            task.put("dag", dag);
            parsedTpTasks.put((String) task.get("id"), JsonUtil.toJson(task));
        }
        return parsedTpTasks;
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
        Map<String, Object> content = parseMap(getFromMap(project, KeyWords.CONTENT));
        Map<String, Object> contentCollections = parseMap(getFromMap(content, KeyWords.COLLECTIONS));
        resetMap(contentCollections, globalIdMap);

        Map<String, Object> contentMapping = parseMap(getFromMap(content, KeyWords.MAPPINGS));
        resetMap(contentMapping, globalIdMap);

        Set<String> contentMappingKeys = new HashSet<>(contentMapping.keySet());
        for (String key : contentMappingKeys) {
            Map<String, Object> mapping = parseMap(getFromMap(contentMapping, key));
            mapping.put("collectionId", replaceId((String) mapping.get("collectionId"), globalIdMap));
        }
        replaceRelationShipsKey(globalIdMap, content);
    }

    public String addJSNode(String tpTable, String script, String declareScript, List<Map<String, Object>> nodes, String sourceId, List<Map<String, Object>> edges) {
        String jsId = UUID.randomUUID().toString().toLowerCase();
        Map<String, Object> jsNode = new HashMap<>();
        jsNode.put("type", "js_processor");
        jsNode.put("name", tpTable);
        jsNode.put("id", jsId);
        jsNode.put("jsType", 1);
        jsNode.put(PROCESSOR_THREAD_NUM, 1);
        jsNode.put(CATALOG, PROCESSOR);
        jsNode.put(ELEMENT_TYPE, "Node");
        jsNode.put("script", script);
        jsNode.put("declareScript", declareScript);
        nodes.add(jsNode);
        Map<String, Object> edge = new HashMap<>();
        edge.put("source", sourceId);
        edge.put("target", jsId);
        edges.add(edge);
        return jsId;
    }

    public String addRenameNode(String tpTable, List<Map<String, Object>> renameOperations, String sourceId, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> renameNode = new HashMap<>();
        String renameId = UUID.randomUUID().toString().toLowerCase();
        renameNode.put("id", renameId);
        renameNode.put(CATALOG, PROCESSOR);
        renameNode.put(ELEMENT_TYPE, "Node");
        renameNode.put("fieldsNameTransform", "");
        renameNode.put("isTransformed", false);
        renameNode.put("name", "Rename " + tpTable);
        renameNode.put(PROCESSOR_THREAD_NUM, 1);
        renameNode.put("type", "field_rename_processor");
        nodes.add(renameNode);
        renameNode.put("operations", renameOperations);
        Map<String, Object> edge = new HashMap<>();
        edge.put("source", sourceId);
        edge.put("target", renameId);
        edges.add(edge);
        sourceId = renameId;
        return sourceId;
    }

    public String addDeleteNode(String tpTable, List<Map<String, Object>> deleteOperations, String sourceId, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> deleteNode = new HashMap<>();
        String deleteId = UUID.randomUUID().toString().toLowerCase();
        deleteNode.put("id", deleteId);
        deleteNode.put(CATALOG, PROCESSOR);
        deleteNode.put("deleteAllFields", false);
        deleteNode.put(ELEMENT_TYPE, "Node");
        deleteNode.put("name", "Delete " + tpTable);
        deleteNode.put("type", "field_add_del_processor");
        deleteNode.put(PROCESSOR_THREAD_NUM, 1);
        deleteNode.put("operations", deleteOperations);
        nodes.add(deleteNode);
        Map<String, Object> edge = new HashMap<>();
        edge.put("source", sourceId);
        edge.put("target", deleteId);
        edges.add(edge);
        sourceId = deleteId;
        return sourceId;
    }

    public Map<String, Object> getDeleteOperation(Object deleteFieldName, Object isPrimaryKey) {
        Map<String, Object> deleteOperation = new HashMap<>();
        deleteOperation.put("id", UUID.randomUUID().toString().toLowerCase());
        deleteOperation.put("field", deleteFieldName);
        deleteOperation.put("op", "REMOVE");
        deleteOperation.put("operand", "true");
        deleteOperation.put("label", deleteFieldName);
        deleteOperation.put("isPk", isPrimaryKey);
        return deleteOperation;
    }

    public Map<String, Object> getRenameOperation(Object source, Object target) {
        Map<String, Object> fieldRenameOperation = new HashMap<>();
        fieldRenameOperation.put("id", UUID.randomUUID().toString().toLowerCase());
        fieldRenameOperation.put("field", source);
        fieldRenameOperation.put("op", "RENAME");
        fieldRenameOperation.put("operand", target);
        return fieldRenameOperation;
    }

    public Map<String, Object> getNewNameMap(Map<String, Object> target, Map<String, Object> source) {
        Map<String, Object> newName = new HashMap<>();
        newName.put("target", target.get("name").toString());
        newName.put("isPrimaryKey", (Boolean) source.get("isPrimaryKey"));
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
            case 3:
                tablePathInfo.setDatabase(tablePathAfterSplit[0]);
                tablePathInfo.setSchema(tablePathAfterSplit[1]);
                tablePathInfo.setTable(tablePathAfterSplit[2]);
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

    public void genProperties(Map<String, Object> parent, Map<String, Object> contentMapping, Map<String, Object> relationshipsMapping, Map<String, Object> full, Map<String, String> sourceToJS, Map<String, Map<String, Map<String, Object>>> renameFields, Map<String, List<Map<String, Object>>> contentDeleteOperations, Map<String, List<Map<String, Object>>> contentRenameOperations) {
        String parentId = (String) parent.get(RM_ID_KEY);
        List<String> children = (List<String>) (parseMap(getFromMap(relationshipsMapping, parentId))).get(KeyWords.CHILDREN);
        if (children == null || children.size() == 0) {
            return;
        }
        List<Map<String, Object>> childrenNode = new ArrayList<>();
        for (String child : children) {
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

            String parentTargetPath = (String) parent.get(KeyWords.TARGET_PATH);
            if (parentTargetPath == null) {
                parentTargetPath = KeyWords.EMPTY;
            }
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
                    if (!KeyWords.EMPTY.equals(parentTargetPath)) {
                        targetPath = embeddedPath;
                    } else {
                        targetPath = String.format(KeyWords.FROMAT, parentTargetPath, embeddedPath) ;
                    }
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
            childNode.put(RM_ID_KEY, child);

            // 由于使用外键做关联, 所以似乎 RM 只能合并来自一个源的数据, 所以 tables 表结构使用其中一个就可以

            Map<String, Map<String, String>> sourceJoinKeyMapping = new HashMap<>();
            Map<String, Map<String, String>> targetJoinKeyMapping = new HashMap<>();
            for (String columnKey : currentColumns.keySet()) {
                Map<String, Object> column = parseMap(getFromMap(currentColumns, columnKey));
                Map<String, Object> foreignKey = parseMap(getFromMap(column, KeyWords.FOREIGN_KEY));
                if (foreignKey == null) {
                    continue;
                }
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
            }

            parentColumnsFindJoinKeys(parent, renameFields, parentColumns, tpTable, joinKeys, sourceJoinKeyMapping, targetJoinKeyMapping);
            childNode.put(KeyWords.JOIN_KEYS, joinKeys);
            joinKeys.forEach(joinKeyMap -> {
                String sourceJoinKey = joinKeyMap.get(KeyWords.SOURCE);
                addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations, contentRenameOperations, child, sourceJoinKeyMapping, sourceJoinKey);
                String targetJoinKey = joinKeyMap.get(KeyWords.TARGET);
                addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations, contentRenameOperations, parentId, targetJoinKeyMapping, targetJoinKey);
            });
            genProperties(childNode, contentMapping, relationshipsMapping, full, sourceToJS, renameFields, contentDeleteOperations, contentRenameOperations);
            childrenNode.add(childNode);
        }
        parent.put(KeyWords.CHILDREN, childrenNode);
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
        Map<String, Object> relationships = content.get("relationships") == null ? new HashMap<>() : parseMap(getFromMap(content, "relationships"));
        Map<String, Object> relationshipsCollection = relationships.get("collections") == null ? new HashMap<>() : parseMap(getFromMap(relationships, "collections"));
        Set<String> relationshipsCollectionKeys = new HashSet<>(relationshipsCollection.keySet());
        Map<String, Object> relationshipsMapping = relationships.get("mappings") == null ? new HashMap<>() : parseMap(getFromMap(relationships, KeyWords.MAPPINGS));
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
            collection.put("mappings", newMappingIds);
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
            List<String> oldChildren = (List<String>) mapping.get("children");
            List<String> newChildren = new ArrayList<>();
            for (String oldChild : oldChildren) {
                newChildren.add(replaceId(oldChild, globalIdMap));
            }
            mapping.put("children", newChildren);
        }
    }

    public Map<String, Object> getTableSchema(Map<String, Object> full, TablePathInfo tableInfo) {
        Map<String, Object> databasesLayer = parseMap(getFromMap(full, "databases"));
        Map<String, Object> currentDatabaseSchema = parseMap(getFromMap(databasesLayer, tableInfo.getDatabase()));
        Map<String, Object> schemasLayer = parseMap(getFromMap(currentDatabaseSchema, "schemas"));
        Map<String, Object> currentSchema = parseMap(getFromMap(schemasLayer, tableInfo.getSchema()));
        return parseMap(getFromMap(currentSchema, "tables"));
    }

    public String getEmbeddedDocumentPath(String parentTargetPath, Map<String, Object> setting) {
        String targetPath;
        if (parentTargetPath.equals("")) {
            targetPath = String.valueOf(setting.get("embeddedPath"));
        } else {
            Object embeddedPath = setting.get("embeddedPath");
            if (null == embeddedPath || StringUtils.isBlank(String.valueOf(embeddedPath))) {
                targetPath = parentTargetPath;
            } else {
                targetPath = parentTargetPath + "." + String.valueOf(embeddedPath);
            }
        }
        return targetPath;
    }

    public void parentColumnsFindJoinKeys(Map<String, Object> parent, Map<String, Map<String, Map<String, Object>>> renameFields, Map<String, Object> parentColumns, String tpTable, List<Map<String, String>> joinKeys, Map<String, Map<String, String>> souceJoinKeyMapping, Map<String, Map<String, String>> targetJoinKeyMapping) {
        Map<String, Map<String, Object>> parentRenameFields = renameFields.get((String) parent.get("tableName"));
        for (String columnKey : parentColumns.keySet()) {
            Map<String, Object> column = parseMap(getFromMap(parentColumns, columnKey));
            Map<String, Object> foreignKey = parseMap(getFromMap(column, "foreignKey"));
            if (foreignKey == null) {
                continue;
            }
            if (null != tpTable && tpTable.equals(foreignKey.get("table"))) {
                Map<String, String> joinKey = new HashMap<>();
                String sourceJoinKey = renameFields.get(tpTable).get(((String) foreignKey.get("column"))).get("target").toString();
                Map<String, String> sourceNewFieldMap = new HashMap<>();
                sourceNewFieldMap.put("source", (String) foreignKey.get("column"));
                sourceNewFieldMap.put("target", sourceJoinKey);
                souceJoinKeyMapping.put(sourceJoinKey, sourceNewFieldMap);
                joinKey.put("source", sourceJoinKey);
                Map<String, String> targetNewFieldMap = new HashMap<>();
                String targetJoinKey = parentRenameFields.get(columnKey).get("target").toString();
                targetNewFieldMap.put("source", columnKey);
                targetNewFieldMap.put("target", targetJoinKey);
                String targetPath = parent.get("targetPath").toString();
                if (!StringUtils.isBlank(targetPath)) {
                    targetJoinKey = targetPath + "." + targetJoinKey;
                }
                joinKey.put("target", targetJoinKey);
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
            Map<String, Object> renameOperation = getRenameOperation(joinKeyMapping.get(joinKey).get("source"), joinKeyMapping.get(joinKey).get("target"));
            childRenameOperations.add(renameOperation);
        }
    }

    public boolean removeDeleteOperation(List<Map<String, Object>> deleteOperations, Map<String, Map<String, String>> joinKeyMapping, String joinKey) {
        return deleteOperations.removeIf((delOperations) -> {
            String deleteField = (String) delOperations.get("field");
            Map<String, String> joinKeyInfo = joinKeyMapping.get(joinKey);
            if (null != joinKeyInfo) {
                String originalField = joinKeyInfo.get("source");
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
