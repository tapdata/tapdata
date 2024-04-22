package com.tapdata.tm.commons.util;

import cn.hutool.core.util.ObjectUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CustomKafkaUtils {

	public static void updateSourceMetadataInstances(List<MetadataInstancesDto> metadataList, Node node, DataSourceConnectionDto sourceConnectionDto, List<String> customKafkaQualifiedNames, BiFunction<MetadataInstancesDto, String, MetadataInstancesDto> fun) {
		if (null == sourceConnectionDto) return;
		if (node instanceof TableNode) {
			// 开发任务
			List<Node> targets = node.getDag().getTargets();
			if (null == targets || targets.isEmpty()) throw new RuntimeException("not found target node");
			Node targetNode = targets.get(0);
			if (targetNode instanceof TableNode) {
				String tableName = ((TableNode) targetNode).getTableName();
				MetadataInstancesDto metadataInstancesDto = getMetadaInstancesDto(metadataList, targetNode, tableName);
				if (null == metadataInstancesDto) throw new RuntimeException("not found metadata instance");
				if (customKafkaQualifiedNames.isEmpty()) throw new RuntimeException("not found kafka qualified name");

				String qualifiedName = customKafkaQualifiedNames.get(0);
				MetadataInstancesDto sourceMetadataInstancesDto = metadataList.stream().filter(dto -> {
					return dto.getQualifiedName().equals(qualifiedName);
				}).findFirst().orElseGet(() -> {
					throw new RuntimeException("Not found source schema: " + qualifiedName);
				});

				sourceMetadataInstancesDto.setFields(metadataInstancesDto.getFields());
				MetadataInstancesDto applyDto = fun.apply(sourceMetadataInstancesDto, sourceConnectionDto.getDatabase_type());
				Optional.ofNullable(applyDto.getFields()).ifPresent(fields -> {
					for (Field field : fields) {
						field.setSourceDbType(sourceConnectionDto.getDatabase_type());
					}
					sourceMetadataInstancesDto.setFields(fields);
				});
				return;
			}

			throw new RuntimeException("not support kafka target node type: " + targetNode.getType());
		} else if (node instanceof DatabaseNode) {
//			// 复制任务
//			for (String name : customKafkaQualifiedNames) {
//				String tableName = CustomKafkaUtils.getQualifiedNameTableName(sourceConnectionDto, name);
//				MetadataInstancesDto metadaInstancesDto = getMetadaInstancesDto(metadataList, node.getId(), tableName);
//				if (null == metadaInstancesDto) throw new RuntimeException("not found metadata instance");
//				metadataList.add(CustomKafkaUtils.parse2SourceMetadaInstancesDo(sourceConnectionDto, tableName, metadaInstancesDto, node));
//			}
//			return;
		}
		throw new RuntimeException("not support kafka source node type: " + node.getType());
	}

	public static void generateSourceMetadataInstances(List<MetadataInstancesDto> metadataList, Node node, DataSourceConnectionDto sourceConnectionDto, List<String> customKafkaQualifiedNames) {
		if (null == sourceConnectionDto) return;

		if (node instanceof TableNode) {
			// 开发任务
			List<Node> targets = node.getDag().getTargets();
			if (null == targets || targets.isEmpty()) throw new RuntimeException("not found target node");

			Node targetNode = targets.get(0);
			if (targetNode instanceof TableNode) {
				String tableName = ((TableNode) targetNode).getTableName();
				MetadataInstancesDto metadataInstancesDto = getMetadaInstancesDto(metadataList, targetNode, tableName);
				if (null == metadataInstancesDto) throw new RuntimeException("not found metadata instance");
				if (customKafkaQualifiedNames.isEmpty()) throw new RuntimeException("not found kafka qualified name");

				String qualifiedName = customKafkaQualifiedNames.get(0);
				metadataList.add(CustomKafkaUtils.parse2SourceMetadaInstancesDo(sourceConnectionDto, qualifiedName, metadataInstancesDto, node));
				return;
			}

			throw new RuntimeException("not support kafka target node type: " + targetNode.getType());
		} else if (node instanceof DatabaseNode) {
//			// 复制任务
//			for (String name : customKafkaQualifiedNames) {
//				String tableName = CustomKafkaUtils.getQualifiedNameTableName(sourceConnectionDto, name);
//				MetadataInstancesDto metadaInstancesDto = getMetadaInstancesDto(metadataList, tableName);
//				if (null == metadaInstancesDto) throw new RuntimeException("not found metadata instance");
//				metadataList.add(CustomKafkaUtils.parse2SourceMetadaInstancesDo(sourceConnectionDto, tableName, metadaInstancesDto, node));
//			}
//			return;
		}
		throw new RuntimeException("not support kafka source node type: " + node.getType());
	}

	private static MetadataInstancesDto getMetadaInstancesDto(List<MetadataInstancesDto> metadataList, Node targetNode, String tableName) {
		for (MetadataInstancesDto metadataInstancesDto : metadataList) {
			if (targetNode instanceof TableNode) {
				TableNode targetTableNode = (TableNode) targetNode;
				boolean isTargetNodeMetadata = targetTableNode.getId().equals(metadataInstancesDto.getNodeId()) || targetTableNode.getConnectionId().equals(metadataInstancesDto.getSource().get_id());
				if (isTargetNodeMetadata && metadataInstancesDto.getName().equals(tableName)) {
					return metadataInstancesDto;
				}
			}
		}
		return null;
	}

	public static boolean checkSourceIsKafka(Node node) {
		Set<String> mqSet = new HashSet<>();
		mqSet.add("Kafka");
		boolean nodeIsTarget;
		boolean enableUseTargetSchema = false;
		if (node instanceof TableNode) {
			if (null != ((TableNode) node).getNodeConfig()) {
				enableUseTargetSchema = Boolean.TRUE.equals(((TableNode) node).getNodeConfig().get("enableUseTargetSchema"));
			}
			List<String> targetNodes = node.getDag().getTargets().stream().map((node1) -> node1.getId()).collect(Collectors.toList());
			nodeIsTarget = targetNodes.contains(node.getId());
		} else {
			if (null != ((DatabaseNode) node).getNodeConfig()) {
				enableUseTargetSchema = Boolean.TRUE.equals(((DatabaseNode) node).getNodeConfig().get("enableUseTargetSchema"));
			}
			DatabaseNode targetNode = node.getDag().getTargetNode(node.getId());
			nodeIsTarget = null != targetNode && targetNode.getId().equals(node.getId());
		}
		boolean isKafkaNode;
		if (node instanceof TableNode) {
			isKafkaNode = mqSet.contains(((TableNode) node).getDatabaseType());
		} else {
			isKafkaNode = mqSet.contains(((DatabaseNode) node).getDatabaseType());
		}
		return isKafkaNode && !nodeIsTarget && enableUseTargetSchema;
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

	public static MetadataInstancesDto parse2SourceMetadaInstancesDo(DataSourceConnectionDto sourceConnectionDto, String qualifiedName, MetadataInstancesDto metadataInstancesDto, Node node) {
		String tableName = CustomKafkaUtils.getQualifiedNameTableName(sourceConnectionDto, qualifiedName);
		MetadataInstancesDto resultDto = ObjectUtil.clone(metadataInstancesDto);

		// 设置数据源
		SourceDto kafkaSourceNode = new SourceDto();
		BeanUtils.copyProperties(sourceConnectionDto, kafkaSourceNode);
		kafkaSourceNode.set_id(sourceConnectionDto.getId().toHexString());
		resultDto.setSource(kafkaSourceNode);

		resultDto.setId(null);
		resultDto.setName(tableName);
		resultDto.setPdkGroup(sourceConnectionDto.getDefinitionGroup());
		resultDto.setPdkVersion(sourceConnectionDto.getDefinitionVersion());
		resultDto.setPdkId(sourceConnectionDto.getDefinitionPdkId());
		resultDto.setFields(metadataInstancesDto.getFields());
		resultDto.setMetaType(metadataInstancesDto.getMetaType());
		resultDto.setOriginalName(tableName);
		resultDto.setAncestorsName(tableName);
		resultDto.setNodeId(node.getId());
		resultDto.setDatabaseId(sourceConnectionDto.getId().toHexString());
		resultDto.setQualifiedName(qualifiedName);
		if (node instanceof TableNode) {
			TableNode tableNode = (TableNode) node;
			resultDto.setConnectionId(tableNode.getConnectionId());
		} else {
			DatabaseNode databaseNode = (DatabaseNode) node;
			resultDto.setConnectionId(databaseNode.getConnectionId());
		}
		return resultDto;
	}
}
