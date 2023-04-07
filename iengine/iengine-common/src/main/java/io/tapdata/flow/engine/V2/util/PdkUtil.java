package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.pdk.apis.context.ConfigContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author jackin
 * @date 2022/2/25 10:10 PM
 **/
public class PdkUtil {

	private static final Map<String, Object> pdkHashDownloadLockMap = new ConcurrentHashMap<>();
	private static final String TAG;

	static {
		TAG = PdkUtil.class.getSimpleName();
	}

	public static Object pdkDownloadLock(String pdkHash) {
		Object lock = pdkHashDownloadLockMap.get(pdkHash);
		if(lock == null) {
			return pdkHashDownloadLockMap.computeIfAbsent(pdkHash, s -> new int[0]);
		}
		return lock;
	}

	public static boolean pdkDownloadUnlock(String pdkHash, Object lock) {
		return pdkHashDownloadLockMap.remove(pdkHash, lock);
	}

	public static void downloadPdkFileIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, String resourceId) {
		final Object lock = pdkDownloadLock(pdkHash);
		synchronized (lock) {
			try {
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
								put("pdkBuildNumber", CommonUtils.getPdkBuildNumer());
							}},
							"pdk/jar/v2",
							filePath.toString(),
							false
					);

					PDKIntegration.refreshJars(filePath.toString());
				} else if (!PDKIntegration.hasJar(theFilePath.getName())) {
					PDKIntegration.refreshJars(filePath.toString());
				}
			} finally {
				pdkDownloadUnlock(pdkHash, lock);
			}
		}
	}

	@NotNull
	public static String encodeOffset(Object offsetObject) {
		if (null != offsetObject) {
			byte[] offsetBytes = InstanceFactory.instance(ObjectSerializable.class).fromObject(offsetObject);
			if(offsetBytes == null)
				TapLogger.error(TAG, "Serialize offsetObject {} failed, as returned null", offsetObject);
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
										   KVReadOnlyMap<TapTable> pdkTableMap,
										   PdkStateMap pdkStateMap,
										   PdkStateMap globalStateMap,
										   Log log) {
		return createNode(dagId, databaseType, clientMongoOperator, associateId, connectionConfig, pdkTableMap, pdkStateMap, globalStateMap, null, log);
	}
	public static ConnectorNode createNode(String dagId,
										   DatabaseTypeEnum.DatabaseType databaseType,
										   ClientMongoOperator clientMongoOperator,
										   String associateId,
										   Map<String, Object> connectionConfig,
										   KVReadOnlyMap<TapTable> pdkTableMap,
										   PdkStateMap pdkStateMap,
										   PdkStateMap globalStateMap,
										   ConfigContext configContext,
										   Log log) {
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
				configContext,
				log
		);
	}

	public static ConnectorNode createNode(
			String dagId,
			DatabaseTypeEnum.DatabaseType databaseType,
			ClientMongoOperator clientMongoOperator,
			String associateId,
			Map<String, Object> connectionConfig,
			Map<String, Object> nodeConfig,
			KVReadOnlyMap<TapTable> pdkTableMap,
			PdkStateMap pdkStateMap,
			PdkStateMap globalStateMap,
			ConnectorCapabilities connectorCapabilities,
			ConfigContext configContext,
			Log log
	) {
		ConnectorNode connectorNode;
		try {
			downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
			PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
					.withLog(log)
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