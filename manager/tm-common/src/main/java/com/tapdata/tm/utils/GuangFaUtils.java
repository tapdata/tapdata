package com.tapdata.tm.utils;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * 广发工具类，用于识别、处理广发相关定制逻辑
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/22 14:39 Create
 */
public class GuangFaUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(GuangFaUtils.class);

	private static final String ENV_PROPERTY_NAME = "TAPD8_GUANG_FA";
	private static Boolean isGuangFa = null;

	private GuangFaUtils() {
	}

	private static String getConf(String key) {
		String tmp = System.getenv(key);
		if (null == tmp) {
			tmp = System.getProperty(key);
		}
		return tmp;
	}

	private static String getConf(String key, String def) {
		String tmp = getConf(key);
		if (null == tmp) {
			tmp = def;
		}
		return tmp;
	}

	public static boolean isGuangFa() {
		if (null != isGuangFa) return isGuangFa;

		synchronized (GuangFaUtils.class) {
			if (null == isGuangFa) {
				String guangfaTag = getConf(ENV_PROPERTY_NAME);
				isGuangFa = null != guangfaTag;
				if (isGuangFa) {
					LOGGER.info("Use GuangFa configuration.");
				}
			}
		}
		return isGuangFa;
	}


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

}
