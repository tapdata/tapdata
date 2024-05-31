package com.tapdata.tm.commons.dag.process;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.error.TapDynamicTableNameExCode_35;
import io.tapdata.exception.TapCodeException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@NodeType("table_rename_processor")
@Getter
@Setter
@Slf4j
public class TableRenameProcessNode extends MigrateProcessorNode {
    /**
     * 创建处理器节点
     **/
    public TableRenameProcessNode() {
        super(NodeEnum.table_rename_processor.name(), NodeCatalog.processor);
    }

    /**
     * 源表名-新表名
     */
    private LinkedHashSet<TableRenameTableInfo> tableNames;


    public void setTableNames(LinkedHashSet<TableRenameTableInfo> tableNames) {
        this.tableNames = tableNames;
    }


    private String prefix;
    private String suffix;
    private String replaceBefore;
    private String replaceAfter;
    private String transferCase;


    public Map<String, TableRenameTableInfo> originalMap() {
        if (null == tableNames) {
            return Maps.newLinkedHashMap();
        }

        return tableNames.stream().collect(Collectors.toMap(TableRenameTableInfo::getOriginTableName, Function.identity()));
    }

    public Map<String, TableRenameTableInfo> currentMap() {
        if (null == tableNames) {
            return Maps.newLinkedHashMap();
        }

        return tableNames.stream().collect(Collectors.toMap(TableRenameTableInfo::getCurrentTableName, Function.identity()));
    }

    public Map<String, TableRenameTableInfo> previousMap() {
        Map<String, TableRenameTableInfo> resultMap = Maps.newLinkedHashMap();
        if (null == tableNames) {
            return resultMap;
        }

        for (TableRenameTableInfo tableRenameTableInfo : tableNames) {
            resultMap.put(tableRenameTableInfo.getPreviousTableName(), tableRenameTableInfo);
        }
        return resultMap;
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }

        // 'schemas' is null because node not exists physical model
        List<Schema> outputSchemas = SchemaUtils.cloneSchema(inputSchemas.get(0));

        outputSchemas.forEach(schema -> {
            Map<String, TableRenameTableInfo> originaledMap = originalMap();
            String ancestorsName = schema.getAncestorsName();
            String currentTableName = convertTableName(originaledMap, ancestorsName, false);
            schema.setName(currentTableName);
            schema.setOriginalName(currentTableName);
            Optional.ofNullable(options.getIncludes()).ifPresent(includes -> {
                for (int i = 0; i < includes.size(); i++) {
                    if (ancestorsName.equals(includes.get(i))) {
                        includes.set(i, currentTableName);
                    }
                }
            });
        });

        return outputSchemas;
    }


    public String convertTableName(Map<String, TableRenameTableInfo> infoMap, String tableName, boolean isRenameDDL) {
        TableRenameTableInfo tableInfo = infoMap.get(tableName);
        if (null != tableInfo) {
            if (isRenameDDL) {
                throw new TapCodeException(TapDynamicTableNameExCode_35.RENAME_DDL_CONFLICTS_WITH_CUSTOM_TABLE_NAME
                    , String.format("Can't apply table rename DDL because it conflicts with custom-table-name from '%s' to '%s'", tableName, tableInfo.getCurrentTableName())
                );
            }
            String currentTableName = tableInfo.getCurrentTableName();
            if (StringUtils.isNotBlank(currentTableName)) {
                return currentTableName;
            }
            return tableName;
        }
        return convertTableName(tableName);
    }

    public String convertTableName(String tableName) {
        if (StringUtils.isNotBlank(prefix)) {
            tableName = prefix + tableName;
        }

        if (StringUtils.isNotBlank(suffix)) {
            tableName = tableName + suffix;
        }

        if (StringUtils.isNotBlank(replaceBefore) && replaceAfter != null) {
            tableName = tableName.replace(replaceBefore, replaceAfter);
        }

        if (StringUtils.isNotBlank(transferCase)) {
            if (transferCase.equals("toUpperCase")) {
                tableName = tableName.toUpperCase(Locale.ROOT);
            } else if (transferCase.equals("toLowerCase")) {
                tableName = tableName.toLowerCase(Locale.ROOT);
            }
        }
        return tableName;
    }
}
