package com.tapdata.tm.task.utils;

import cn.hutool.core.util.ObjectUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.utils.MongoUtils;

import java.util.*;
import java.util.regex.Pattern;

public class CustomKafkaUtils {
    public static boolean checkSourceIsKafka(Node node){
        Set<String> mqSet = new HashSet<>();
        mqSet.add("Kafka");
        DatabaseNode targetNode = node.getDag().getTargetNode(node.getId());
        boolean nodeIsTarget = targetNode.getId().equals(node.getId());
        boolean isKafkaNode;
        if(node instanceof TableNode){
            isKafkaNode=mqSet.contains(((TableNode) node).getDatabaseType());
        } else{
            isKafkaNode=mqSet.contains(((DatabaseNode)node).getDatabaseType());
        }
        return isKafkaNode&&!nodeIsTarget;
    }
    public static List<String> customKafkaGetTable(DatabaseNode databaseNode) {

        List<LinkedHashMap> targetTableNames = (ArrayList) databaseNode.getNodeConfig().get("table_names");
        List<String> tableNames = new ArrayList<>();
        for (LinkedHashMap<String, String> linkedHashMap : targetTableNames) {
            tableNames.add(linkedHashMap.get("name"));
        }
        return tableNames;
    }

    public static String getQualifiedNameTableName(DataSourceConnectionDto dataSourceConnectionDto, String qualifiedNames) {
        String prefix = String.format("T_%s_%s_%s_", dataSourceConnectionDto.getDefinitionPdkId(), dataSourceConnectionDto.getDefinitionGroup(), dataSourceConnectionDto.getDefinitionVersion());
        String tableName = qualifiedNames.substring(prefix.length());
        tableName = tableName.substring(0, tableName.lastIndexOf("_"));
        tableName = tableName.substring(0, tableName.lastIndexOf("_"));
        return tableName;
    }

    public static MetadataInstancesDto parse2SourceMetadaInstancesDo(DataSourceConnectionDto sourceConnectionDto, String qualifiedName, MetadataInstancesDto metadataInstancesDto,Node node) {
        String tableName = CustomKafkaUtils.getQualifiedNameTableName(sourceConnectionDto, qualifiedName);
        MetadataInstancesDto metadataInstancesDto1 = ObjectUtil.clone(metadataInstancesDto);
        SourceDto kafkaSourceNode = ObjectUtil.clone(metadataInstancesDto1.getSource());
        String connectionId = ((DataParentNode) node).getConnectionId();
        kafkaSourceNode.setId(MongoUtils.toObjectId(connectionId));
        kafkaSourceNode.set_id(connectionId);
        kafkaSourceNode.setDatabase_type(sourceConnectionDto.getDefinitionPdkId());
        kafkaSourceNode.setName(node.getName());
        metadataInstancesDto1.setPdkGroup(sourceConnectionDto.getDefinitionGroup());
        metadataInstancesDto1.setPdkVersion(sourceConnectionDto.getDefinitionVersion());
        metadataInstancesDto1.setPdkId(sourceConnectionDto.getDefinitionPdkId());
        metadataInstancesDto1.setFields(metadataInstancesDto.getFields());
        metadataInstancesDto1.setMetaType(metadataInstancesDto.getMetaType());
        metadataInstancesDto1.setOriginalName(tableName);
        metadataInstancesDto1.setAncestorsName(tableName);
        metadataInstancesDto1.setNodeId(node.getId());
        metadataInstancesDto1.setDatabaseId(sourceConnectionDto.getId().toHexString());
        metadataInstancesDto1.setQualifiedName(qualifiedName);
        metadataInstancesDto1.setSource(kafkaSourceNode);
        if(node instanceof TableNode){
            TableNode tableNode = (TableNode) node;
            metadataInstancesDto1.setConnectionId(tableNode.getConnectionId());
        }else{
            DatabaseNode databaseNode = (DatabaseNode) node;
            metadataInstancesDto1.setConnectionId(databaseNode.getConnectionId());
        }
        return metadataInstancesDto1;
    }
}
