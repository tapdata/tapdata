package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.StringCompression;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.flow.engine.V2.entity.SyncProgressNodeType;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jackin
 * @date 2022/2/22 5:04 PM
 **/
public abstract class HazelcastDataBaseNode extends HazelcastBaseNode {
	protected static final String STREAM_OFFSET_COMPRESS_PREFIX = "_tap_zip_";
	protected static final String STREAM_OFFSET_COMPRESS_PREFIX_V2 = "_v2_tap_zip_";

	protected SyncTypeEnum syncType;
	protected DataProcessorContext dataProcessorContext;
	protected Throwable offsetFromTimeError;

	public HazelcastDataBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.dataProcessorContext = dataProcessorContext;
		this.syncType = SyncTypeEnum.get(dataProcessorContext.getTaskDto().getType());
	}

	@SneakyThrows
	protected boolean need2InitialSync(SyncProgress syncProgress) {
		if (!isRunning()) {
			return false;
		}
		if (SyncTypeEnum.INITIAL_SYNC != syncType && SyncTypeEnum.INITIAL_SYNC_CDC != syncType) {
			return false;
		}
		if (syncProgress != null) {

			if (syncProgress.getBatchOffsetObj() != null && syncProgress.getBatchOffsetObj() instanceof Map) {
				AtomicBoolean hasRunningTable = new AtomicBoolean(false);
				((Map<?,?>)syncProgress.getBatchOffsetObj()).keySet().forEach(key -> {
					if (!BatchOffsetUtil.batchIsOverOfTable(syncProgress, String.valueOf(key))) {
						hasRunningTable.set(true);
					}
				});
				if (hasRunningTable.get()) {
					return true;
				}
			}

			String syncStage = syncProgress.getSyncStage();
			if (StringUtils.isNotBlank(syncStage)
					&& SyncStage.valueOf(syncStage).equals(SyncStage.CDC)) {
				return false;
			}
		}

		return true;
	}

	@SneakyThrows
	protected boolean need2CDC() {
		if (SyncTypeEnum.CDC != syncType && SyncTypeEnum.INITIAL_SYNC_CDC != syncType) {
			return false;
		}

		return true;
	}

	public Map<String,SyncProgress> foundAllSyncProgress(Map<String, Object> attrs) {
		Map<String, SyncProgress> allSyncProgressMap = new HashMap<>();
		try {
			if (org.apache.commons.collections.MapUtils.isEmpty(attrs)) {
				return allSyncProgressMap;
			}
			Object syncProgressObj = attrs.get("syncProgress");
			if (syncProgressObj instanceof Map) {
				for (Map.Entry<?, ?> entry : ((Map<?, ?>) syncProgressObj).entrySet()) {
					Object key = entry.getKey();
					Object syncProgressString = entry.getValue();
					if (!(key instanceof String) || !(syncProgressString instanceof String)) {
						continue;
					}
					List<String> keyList;
					try {
						keyList = JSONUtil.json2List((String) key, String.class);
					} catch (IOException e) {
						throw new RuntimeException("Convert key to list failed. Key string: " + key + "; Error: " + e.getMessage(), e);
					}
					if (CollectionUtils.isNotEmpty(keyList)) {
						try {
							String syncProgressKey = String.join(",", keyList);
							SyncProgress syncProgress = JSONUtil.json2POJO((String) syncProgressString, new TypeReference<SyncProgress>() {
							});
							allSyncProgressMap.put(syncProgressKey, syncProgress);
						} catch (IOException e) {
							throw new RuntimeException("Convert sync progress json to pojo failed. Sync progress string: " + syncProgressString
									+ "; Error: " + e.getMessage(), e);
						}
					}
				}
			} else {
				if (null == syncProgressObj) {
					obsLogger.trace("Sync progress not exists, will run task as first time");
				} else {
					throw new RuntimeException("Unrecognized sync progress type: " + syncProgressObj.getClass().getName() + ", should be a map");
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Init sync progress failed; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
		}
		return allSyncProgressMap;
	}

	protected SyncProgress foundNodeSyncProgress(Map<String, SyncProgress> allSyncProgress, SyncProgressNodeType syncProgressNodeType) {
		SyncProgress syncProgress = null;
		for (Map.Entry<String, SyncProgress> entry : allSyncProgress.entrySet()) {
			String key = entry.getKey();
			String[] nodeIds = key.split(",");
			if (getNode().getId().equals(nodeIds[syncProgressNodeType.getIndex()])) {
				SyncProgress temp = entry.getValue();
				if (null == syncProgress) {
					syncProgress = temp;
				} else if (temp.compareTo(syncProgress) < 0) {
					syncProgress = temp;
				}
			}
		}
		if (null != syncProgress) {
			if (null == syncProgress.getEventSerialNo()) {
				syncProgress.setEventSerialNo(0L);
			}
		}
		return syncProgress;
	}

	@Nullable
	protected String uncompressStreamOffsetIfNeed(String streamOffsetStr) {
		if (StringUtils.startsWith(streamOffsetStr, STREAM_OFFSET_COMPRESS_PREFIX_V2)) {
			try {
				streamOffsetStr = StringCompression.uncompressV2(StringUtils.removeStart(streamOffsetStr, STREAM_OFFSET_COMPRESS_PREFIX_V2));
			} catch (IOException | IllegalArgumentException e) {
				throw new RuntimeException("Uncompress stream offset failed: " + streamOffsetStr, e);
			}
		} else if (StringUtils.startsWith(streamOffsetStr, STREAM_OFFSET_COMPRESS_PREFIX)) {
			try {
				streamOffsetStr = StringCompression.uncompress(StringUtils.removeStart(streamOffsetStr, STREAM_OFFSET_COMPRESS_PREFIX));
			} catch (IOException | IllegalArgumentException e) {
				throw new RuntimeException("Uncompress stream offset failed: " + streamOffsetStr, e);
			}
		}
		return streamOffsetStr;
	}

	public DataProcessorContext getDataProcessorContext() {
		return dataProcessorContext;
	}
}
