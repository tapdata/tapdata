package io.tapdata.autoinspect.compare;

import com.tapdata.tm.autoinspect.compare.IQueryCompare;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import lombok.NonNull;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/6 11:16 Create
 */
public abstract class QueryCompare implements IQueryCompare {

	@Override
	public Status queryCompare(@NonNull TaskAutoInspectResultDto dto) {

		LinkedHashSet<String> keyNames = new LinkedHashSet<>(dto.getTargetKeymap().keySet());
		CompareRecord sourceRecord = dto.toSourceRecord();

		// refresh target record and compare
		CompareRecord targetRecord = queryTargetByKey(dto.getTargetTableName(), dto.getTargetKeymap(), keyNames);
		if (null == targetRecord) {
			// ignore if record not exists in target and source
			sourceRecord = querySourceByKey(sourceRecord.getTableName(), sourceRecord.getOriginalKey(), keyNames);
			if (null == sourceRecord) {
				return Status.Deleted;
			}
			dto.setSourceData(sourceRecord.getData());
		} else if (CompareStatus.Ok == sourceRecord.compare(targetRecord)) {
			return Status.FixTarget;
		} else {
			// refresh target record to result
			dto.fillTarget(targetRecord);

			// refresh source record and compare
			sourceRecord = querySourceByKey(sourceRecord.getTableName(), sourceRecord.getOriginalKey(), keyNames);
			if (null != sourceRecord) {
				if (CompareStatus.Ok == sourceRecord.compare(targetRecord)) {
					return Status.FixSource;
				}

				// refresh source record to result
				dto.setSourceData(sourceRecord.getData());
			}
		}

		return Status.Diff;
	}

	/**
	 * Query source record
	 *
	 * @param tableName table name
	 * @param keymap    original keymap
	 * @param keys      target keys
	 * @return source record
	 */
	protected abstract CompareRecord querySourceByKey(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> keymap, @NonNull LinkedHashSet<String> keys);

	/**
	 * Query target record
	 *
	 * @param tableName table name
	 * @param keymap    original keymap
	 * @param keys      target keys
	 * @return target record
	 */
	protected abstract CompareRecord queryTargetByKey(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> keymap, @NonNull LinkedHashSet<String> keys);
}
