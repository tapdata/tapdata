package io.tapdata.flow.engine.V2.policy;

import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.function.ThrowableFunction;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.TapPdkViolateUniqueEx;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 10:52
 **/
public class PDkNodeInsertRecordPolicyService extends NodeWritePolicyService {
	public static final int DEFAULT_DUPLICATE_KEY_ERROR_THRESHOLD = 10;
	public static final String WRITE_DUPLICATE_KEY_ERROR_THRESHOLD_PROP_KEY = "WRITE_DUPLICATE_KEY_ERROR_THRESHOLD";
	private final ObsLogger obsLogger;
	private final int writeDuplicateKeyErrorThreshold;
	private DmlPolicyEnum settingInsertPolicy;
	private final String associateId;
	private final Map<String, WriteRecordTableResult> writeRecordTableResultMap = new ConcurrentHashMap<>();

	public PDkNodeInsertRecordPolicyService(TaskDto taskDto, Node<?> node, String associateId) {
		super(taskDto, node);
		this.associateId = associateId;
		if (node instanceof DataParentNode) {
			DmlPolicy dmlPolicy = ((DataParentNode<?>) node).getDmlPolicy();
			settingInsertPolicy = dmlPolicy.getInsertPolicy();
		}
		obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto.getId().toString(), node.getId());
		writeDuplicateKeyErrorThreshold = CommonUtils.getPropertyInt(WRITE_DUPLICATE_KEY_ERROR_THRESHOLD_PROP_KEY, DEFAULT_DUPLICATE_KEY_ERROR_THRESHOLD);
	}

	@Override
	public void writeRecordWithPolicyControl(String tableId, List<TapRecordEvent> tapRecordEvents, ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writePolicyRunner) throws Throwable {
		if (CollectionUtils.isEmpty(tapRecordEvents) || null == settingInsertPolicy || !settingInsertPolicy.equals(DmlPolicyEnum.update_on_exists)) {
			writePolicyRunner.apply(tapRecordEvents);
			return;
		}
		WriteRecordTableResult writeRecordTableResult = writeRecordTableResultMap.computeIfAbsent(tableId, k -> new WriteRecordTableResult());
		ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
		DmlPolicyEnum currentInsertPolicy = DmlPolicyEnum.just_insert;
		if (writeRecordTableResult.getContinuousDuplicateKeyErrorOverLimit().get() ||
				writeRecordTableResult.getDuplicateKeyErrorCounter() > writeDuplicateKeyErrorThreshold) {
			if (writeRecordTableResult.getContinuousDuplicateKeyErrorOverLimit().compareAndSet(false, true)) {
				Optional.ofNullable(obsLogger).ifPresent(log -> log.info("Table '{}' has more than {} continuous duplicate key errors, all subsequent data insert policy are switched to {}",
						tableId, writeDuplicateKeyErrorThreshold, settingInsertPolicy.name()));
			}
			currentInsertPolicy = settingInsertPolicy;
		}
		connectorNode.getConnectorContext().getConnectorCapabilities().alternative(ConnectionOptions.DML_INSERT_POLICY, currentInsertPolicy.name());

		try {
			writePolicyRunner.apply(tapRecordEvents);
			if (DmlPolicyEnum.just_insert.equals(currentInsertPolicy)) {
				writeRecordTableResult.resetDuplicateKeyErrorCounter();
			}
		} catch (Throwable e) {
			Throwable matchThrowable = CommonUtils.matchThrowable(e, TapPdkViolateUniqueEx.class);
			if (null != matchThrowable) {
				connectorNode.getConnectorContext().getConnectorCapabilities().alternative(ConnectionOptions.DML_INSERT_POLICY, settingInsertPolicy.name());
				writePolicyRunner.apply(tapRecordEvents);
				Optional.ofNullable(obsLogger).ifPresent(log -> log.info("Table '{}' has duplicate key error, switch the insert policy to {} and retry writing, continuous error time: {}",
						tableId, settingInsertPolicy.name(), writeRecordTableResult.incrementDuplicateKeyErrorCounter()));
			} else {
				throw e;
			}
		}
	}
}
