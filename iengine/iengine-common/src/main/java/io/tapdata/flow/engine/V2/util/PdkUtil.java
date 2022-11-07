package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.pdk.apis.context.ConfigContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.schema.PdkTableMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jackin
 * @date 2022/2/25 10:10 PM
 **/
public class PdkUtil {

	public static void downloadPdkFileIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, String resourceId) {
		// create the dir used for storing the pdk jar file if the dir not exists
		String dir = System.getProperty("user.dir") + File.separator + "dist";
		File folder = new File(dir);
		if (!folder.exists()) {
			folder.mkdirs();
		}

		String filePrefix = fileName.split("\\.jar")[0];
		StringBuilder filePath = new StringBuilder(dir)
				.append(File.separator)
				.append(filePrefix)
				.append("__").append(resourceId).append("__");

		filePath.append(".jar");
		File theFilePath = new File(filePath.toString());
		if (!theFilePath.isFile()) {
			httpClientMongoOperator.downloadFile(
					new HashMap<String, Object>(1) {{
						put("pdkHash", pdkHash);
					}},
					"/pdk/jar",
					filePath.toString(),
					false
			);

//        PDKIntegration.
			PDKIntegration.refreshJars(filePath.toString());

			if (isSnapshot(fileName)) {
				IOFileFilter fileFilter = FileFilterUtils.and(EmptyFileFilter.NOT_EMPTY,
						new WildcardFileFilter("*" + filePrefix + "*"));
				Collection<File> files = FileUtils.listFiles(new File(dir), fileFilter, DirectoryFileFilter.INSTANCE);
				for (File file : files) {
					if (!file.getAbsolutePath().equals(filePath.toString())) {
						FileUtils.deleteQuietly(file);
					}
				}
			}
		} else if (!PDKIntegration.hasJar(theFilePath.getName())) {
			PDKIntegration.refreshJars(filePath.toString());
		}

	}

	private static boolean isSnapshot(String fileName) {
		return StringUtils.isNotBlank(fileName) && StringUtils.endsWith(fileName, "SNAPSHOT.jar");
	}

	@NotNull
	public static String encodeOffset(Object offsetObject) {
		if (null != offsetObject) {
			byte[] offsetBytes = InstanceFactory.instance(ObjectSerializable.class).fromObject(offsetObject);
			return Base64.encodeBase64String(offsetBytes);
		}
		return "";
	}

	public static Object decodeOffset(String offset, ConnectorNode connectorNode) {
		if (StringUtils.isNotBlank(offset)) {
			byte[] bytes = Base64.decodeBase64(offset);
			return InstanceFactory.instance(ObjectSerializable.class)
					.toObject(bytes, new ObjectSerializable.ToObjectOptions().classLoader(connectorNode.getConnectorClassLoader()));
		}
		return null;
	}

	public static ConnectorNode createNode(String dagId,
										   DatabaseTypeEnum.DatabaseType databaseType,
										   ClientMongoOperator clientMongoOperator,
										   String associateId,
										   Map<String, Object> connectionConfig,
										   PdkTableMap pdkTableMap,
										   PdkStateMap pdkStateMap,
										   PdkStateMap globalStateMap) {
		return createNode(dagId, databaseType, clientMongoOperator, associateId, connectionConfig, pdkTableMap, pdkStateMap, globalStateMap, null);
	}
	public static ConnectorNode createNode(String dagId,
										   DatabaseTypeEnum.DatabaseType databaseType,
										   ClientMongoOperator clientMongoOperator,
										   String associateId,
										   Map<String, Object> connectionConfig,
										   PdkTableMap pdkTableMap,
										   PdkStateMap pdkStateMap,
										   PdkStateMap globalStateMap,
										   ConfigContext configContext) {
		return createNode(
				dagId,
				databaseType,
				clientMongoOperator,
				associateId,
				connectionConfig,
				null,
				pdkTableMap,
				pdkStateMap,
				globalStateMap,
				null,
				configContext
		);
	}

	public static ConnectorNode createNode(
			String dagId,
			DatabaseTypeEnum.DatabaseType databaseType,
			ClientMongoOperator clientMongoOperator,
			String associateId,
			Map<String, Object> connectionConfig,
			Map<String, Object> nodeConfig,
			PdkTableMap pdkTableMap,
			PdkStateMap pdkStateMap,
			PdkStateMap globalStateMap,
			ConnectorCapabilities connectorCapabilities,
			ConfigContext configContext
	) {
		ConnectorNode connectorNode;
		try {
			downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
					.withDagId(dagId)
					.withAssociateId(associateId)
					.withConfigContext(configContext)
					.withGroup(databaseType.getGroup())
					.withVersion(databaseType.getVersion())
					.withPdkId(databaseType.getPdkId())
					.withTableMap(pdkTableMap)
					.withStateMap(pdkStateMap)
					.withGlobalStateMap(globalStateMap);
			if (MapUtils.isNotEmpty(connectionConfig)) {
				connectorBuilder.withConnectionConfig(DataMap.create(connectionConfig));
			}
			if (MapUtils.isNotEmpty(nodeConfig)) {
				connectorBuilder.withNodeConfig(new DataMap() {{
					putAll(nodeConfig);
				}});
			}
			if (null != connectorCapabilities) {
				connectorBuilder.withConnectorCapabilities(connectorCapabilities);
			}
			connectorNode = connectorBuilder.build();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create pdk connector node, database type: " + databaseType + ", message: " + e.getMessage(), e);
		}
		return connectorNode;
	}
}
