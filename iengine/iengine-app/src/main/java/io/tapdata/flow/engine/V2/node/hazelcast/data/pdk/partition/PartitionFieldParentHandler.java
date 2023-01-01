package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.simplify.pretty.TypeHandlers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.map;

/**
 * @author aplomb
 */
public class PartitionFieldParentHandler {
	protected TapTable table;
	protected List<String> partitionFields;
	protected Map<String, Integer> dateFieldFactionMap;
	protected TypeHandlers<TapEvent, Void> typeHandlers = new TypeHandlers<>();
	public PartitionFieldParentHandler(TapTable tapTable) {
		table = tapTable;
		TapIndexEx partitionIndex = table.partitionIndex();
		if(partitionIndex == null || partitionIndex.getIndexFields() == null || partitionIndex.getIndexFields().isEmpty()) {
			throw new CoreException(PartitionErrorCodes.PARTITION_INDEX_NULL, "Partition index is illegal for table {}", table);
		}

		LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
		partitionFields = new ArrayList<>();
		for(TapIndexField field : partitionIndex.getIndexFields()) {
			partitionFields.add(field.getName());
			if(nameFieldMap != null) {
				TapField tapField = nameFieldMap.get(field.getName());
				if(tapField != null && tapField.getTapType() != null) {
					if(tapField.getTapType() instanceof TapDateTime) {
						Integer fraction = ((TapDateTime) tapField.getTapType()).getFraction();
						if(fraction == null)
							fraction = 3;
						dateFieldFactionMap.put(field.getName(), fraction);
					} else if(tapField.getTapType() instanceof TapDate || tapField.getTapType() instanceof TapTime || tapField.getTapType() instanceof TapYear) {
						dateFieldFactionMap.put(field.getName(), 3);
					}
				}
			}
		}
	}

	protected Map<String, Object> getKeyFromData(Map<String, Object> before) {
		return getKeyFromData(before, null);
	}
	protected Map<String, Object> getKeyFromData(Map<String, Object> before, Map<String, Object> after) {
		Map<String, Object> map = map();
		if(before != null) {
			for(String field : partitionFields) {
				map.put(field, before.get(field));
			}
		} else if(after != null) {
			for(String field : partitionFields) {
				map.put(field, after.get(field));
			}
		}
		return map;
	}

	/**
	 * As PDK connector may give different types for date field on batch/stream stages.
	 *
	 * Make it the same to DateTime.
	 *
	 * @param data
	 * @return
	 */
	protected Map<String, Object> reviseData(Map<String, Object> data) {
		if(data != null) {
			if(dateFieldFactionMap != null && !dateFieldFactionMap.isEmpty()) {
				for(Map.Entry<String, Integer> entry : dateFieldFactionMap.entrySet()) {
					data.put(entry.getKey(), AnyTimeToDateTime.toDateTime(data.get(entry.getKey()), entry.getValue()));
				}
			}
		}
		return data;
	}
}
