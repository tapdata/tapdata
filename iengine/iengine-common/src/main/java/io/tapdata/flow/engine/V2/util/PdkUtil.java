package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.utils.PdkSourceUtils;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.pdk.apis.context.ConfigContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
		if (lock == null) {
			return pdkHashDownloadLockMap.computeIfAbsent(pdkHash, s -> new int[0]);
		}
		return lock;
	}

	public static boolean pdkDownloadUnlock(String pdkHash, Object lock) {
		return pdkHashDownloadLockMap.remove(pdkHash, lock);
	}
	public static void downloadPdkFileIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, String resourceId) {
		downloadPdkFileIfNeed(httpClientMongoOperator,pdkHash,fileName,resourceId,null);
	}
	public static void downloadPdkFileIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, String resourceId, boolean needRetry) {
		downloadPdkFileIfNeed(httpClientMongoOperator,pdkHash,fileName,resourceId,null, needRetry);
	}
	public static void downloadPdkFileIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, String resourceId, RestTemplateOperator.Callback callback) {
		downloadPdkFileIfNeed(httpClientMongoOperator, pdkHash, fileName, resourceId, callback, true);
	}
	public static void downloadPdkFileIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, String resourceId, RestTemplateOperator.Callback callback, boolean needRetry) {
		final Object lock = pdkDownloadLock(pdkHash);
		synchronized (lock) {
			try {
				// create the dir used for storing the pdk jar file if the dir not exists
				boolean needDownload = true;
				int retries = 0;
				while (needDownload && retries < 3) {
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
					if (callback != null) callback.needDownloadPdkFile(!theFilePath.isFile());
					if (!theFilePath.isFile()) {
						httpClientMongoOperator.downloadFile(
								new HashMap<String, Object>(1) {{
									put("pdkHash", pdkHash);
									put("pdkBuildNumber", CommonUtils.getPdkBuildNumer());
								}},
								"pdk/jar/v2",
								filePath.toString(),
								false,
								callback
						);

						PDKIntegration.refreshJars(filePath.toString());
					} else if (!PDKIntegration.hasJar(theFilePath.getName())) {
						PDKIntegration.refreshJars(filePath.toString());
					}
					if (needRetry) {
						needDownload = reDownloadIfNeed(httpClientMongoOperator, pdkHash, fileName, theFilePath);
					} else {
						needDownload = false;
					}
					retries++;
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				pdkDownloadUnlock(pdkHash, lock);
			}
		}
	}

	public static boolean reDownloadIfNeed(HttpClientMongoOperator httpClientMongoOperator, String pdkHash, String fileName, File theFilePath){
		Map<String, Object> params = new HashMap(1);
		params.put("pdkHash", pdkHash);
		params.put("fileName", fileName);
		params.put("pdkBuildNumber", CommonUtils.getPdkBuildNumer());
		String md5 = httpClientMongoOperator.findOne(params, "/pdk/checkMd5/v3", String.class);
		String theFilePathMd5 = PdkSourceUtils.getFileMD5(theFilePath);
		if (null != md5 && !md5.equals(theFilePathMd5)){
			FileUtils.deleteQuietly(theFilePath);
			return true;
		}
		return false;
	}
	@NotNull
	public static String encodeOffset(Object offsetObject) {
		if (null != offsetObject) {
			byte[] offsetBytes = InstanceFactory.instance(ObjectSerializable.class).fromObject(offsetObject);
			if (offsetBytes == null)
				TapLogger.error(TAG, "Serialize offsetObject {} failed, as returned null", offsetObject);
			return Base64.encodeBase64String(offsetBytes);
		}
		return "";
	}

	public static Object decodeOffset(String offset, ConnectorNode connectorNode) {
		if (StringUtils.isNotBlank(offset) && null != connectorNode) {
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
		return createNode(
				dagId, databaseType, clientMongoOperator, associateId,
				connectionConfig, nodeConfig, pdkTableMap, pdkStateMap,
				globalStateMap, connectorCapabilities, configContext, log, null
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
			Log log,
			TaskDto taskDto
	) {
		ConnectorNode connectorNode;
		try {
			boolean needRetryDownload = null == taskDto || !taskDto.isPreviewTask();
			downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator,
					databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid(),
					needRetryDownload);
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