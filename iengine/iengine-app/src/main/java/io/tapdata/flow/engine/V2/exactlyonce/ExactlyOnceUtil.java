package io.tapdata.flow.engine.V2.exactlyonce;

import com.tapdata.constant.MD5Util;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exception.TapExactlyOnceWriteExCode_22;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-05-15 19:12
 **/
public class ExactlyOnceUtil {
	public static final String EXACTLY_ONCE_CACHE_TABLE_NAME = "_TAP_EXACTLY_ONCE_CACHE";
	public static final String NODE_ID_COL_NAME = "NODE_ID";
	public static final String TABLE_NAME_COL_NAME = "TABLE_NAME";
	public static final String EXACTLY_ONCE_ID_COL_NAME = "EXACTLY_ONCE_ID";
	public static final String TIMESTAMP_COL_NAME = "TIMESTAMP";
	public static final long EXACTLY_ONCE_ID_COL_LENGTH = 500L;

	public static TapTable generateExactlyOnceTable(ConnectorNode connectorNode) {
		TapTable exactlyOnceTable = new TapTable(EXACTLY_ONCE_CACHE_TABLE_NAME);
		exactlyOnceTable.add(new TapField().name(NODE_ID_COL_NAME).tapType(new TapString(50L, false)).primaryKeyPos(1));
		exactlyOnceTable.add(new TapField().name(TABLE_NAME_COL_NAME).tapType(new TapString(200L, false)).primaryKeyPos(2));
		exactlyOnceTable.add(new TapField().name(EXACTLY_ONCE_ID_COL_NAME).tapType(new TapString(EXACTLY_ONCE_ID_COL_LENGTH, false)).primaryKeyPos(3));
		exactlyOnceTable.add(new TapField().name(TIMESTAMP_COL_NAME).tapType(new TapNumber()
				.precision(20)
				.scale(0)
				.minValue(BigDecimal.valueOf(Long.MIN_VALUE))
				.maxValue(BigDecimal.valueOf(Long.MAX_VALUE))).primaryKeyPos(0));
		TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
		TapResult<LinkedHashMap<String, TapField>> tapResult;
		try {
			tapResult = targetTypesGenerator.convert(
					exactlyOnceTable.getNameFieldMap(),
					connectorNode.getConnectorContext().getSpecification().getDataTypesMap(),
					connectorNode.getCodecsFilterManager()
			);
		} catch (Exception e) {
			throw new TapCodeException(TapExactlyOnceWriteExCode_22.TARGET_TYPES_GENERATOR_FAILED, "Name field: " + exactlyOnceTable.getNameFieldMap(), e);
		}
		exactlyOnceTable.setNameFieldMap(tapResult.getData());
		exactlyOnceTable.add(new TapIndex().indexField(new TapIndexField().name("TIMESTAMP").fieldAsc(true)));
		exactlyOnceTable.add(new TapIndex().indexField(new TapIndexField().name("TIMESTAMP").fieldAsc(true)));
		return exactlyOnceTable;
	}

	public static Map<String, Object> generateExactlyOnceCacheRow(String nodeId, String tableName, TapRecordEvent tapRecordEvent, Long referenceTime) {
		Map<String, Object> exactlyOnceCacheRow = new LinkedHashMap<>();
		exactlyOnceCacheRow.put(NODE_ID_COL_NAME, nodeId);
		exactlyOnceCacheRow.put(TABLE_NAME_COL_NAME, tableName);
		String exactlyOnceId = tapRecordEvent.getExactlyOnceId();
		if (StringUtils.isBlank(exactlyOnceId)) {
			throw new TapCodeException(TapExactlyOnceWriteExCode_22.EXACTLY_ONCE_ID_IS_BLANK, "Record event: " + tapRecordEvent);
		}

		if (exactlyOnceId.length() >= EXACTLY_ONCE_ID_COL_LENGTH) {
			exactlyOnceId = MD5Util.crypt(exactlyOnceId, false);
		}
		exactlyOnceCacheRow.put(EXACTLY_ONCE_ID_COL_NAME, exactlyOnceId);
		exactlyOnceCacheRow.put(TIMESTAMP_COL_NAME, referenceTime);
		return exactlyOnceCacheRow;
	}
}
