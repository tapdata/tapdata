package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 * @date 2022/2/22 5:04 PM
 **/
public abstract class HazelcastDataBaseNode extends HazelcastBaseNode {

	private Logger logger = LoggerFactory.getLogger(HazelcastDataBaseNode.class);
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
		if (!isRunning()) {
			return false;
		}
		if (SyncTypeEnum.CDC != syncType && SyncTypeEnum.INITIAL_SYNC_CDC != syncType) {
			return false;
		}

		return true;
	}

	protected SyncProgress foundSyncProgress(Map<String, Object> attrs) {
		SyncProgress syncProgress = null;
		try {
			if (MapUtils.isEmpty(attrs)) {
				return null;
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
					if (CollectionUtils.isNotEmpty(keyList) && keyList.contains(dataProcessorContext.getNode().getId())) {
						try {
							SyncProgress tmp = JSONUtil.json2POJO((String) syncProgressString, new TypeReference<SyncProgress>() {
							});
							if (null == syncProgress) {
								syncProgress = tmp;
							} else if (tmp.compareTo(syncProgress) < 0) {
								syncProgress = tmp;
							}
						} catch (IOException e) {
							throw new RuntimeException("Convert sync progress json to pojo failed. Sync progress string: " + syncProgressString
									+ "; Error: " + e.getMessage(), e);
						}
					}
				}
			} else {
				if (null == syncProgressObj) {
					logger.info("Sync progress not exists, will run task as first time");
				} else {
					throw new RuntimeException("Unrecognized sync progress type: " + syncProgressObj.getClass().getName() + ", should be a map");
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Init sync progress failed; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
		}

		if (null != syncProgress) {
			if (null == syncProgress.getEventSerialNo()) {
				syncProgress.setEventSerialNo(0L);
			}
		}
		return syncProgress;
	}

	public DataProcessorContext getDataProcessorContext() {
		return dataProcessorContext;
	}
}
