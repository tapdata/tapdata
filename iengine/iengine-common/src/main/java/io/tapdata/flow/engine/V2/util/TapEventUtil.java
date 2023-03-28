package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.OperationType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 11:58
 **/
public class TapEventUtil {
	public static Map<String, Object> getBefore(TapEvent tapEvent) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getBefore();
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			return ((TapDeleteRecordEvent) tapEvent).getBefore();
		}
		return null;
	}

	public static void setBefore(TapEvent tapEvent, Map<String, Object> before) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			((TapUpdateRecordEvent) tapEvent).setBefore(before);
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			((TapDeleteRecordEvent) tapEvent).setBefore(before);
		}
	}

	public static Map<String, Object> getAfter(TapEvent tapEvent) {
		if (tapEvent instanceof TapInsertRecordEvent) {
			return ((TapInsertRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getAfter();
		}
		return null;
	}

	public static void setAfter(TapEvent tapEvent, Map<String, Object> after) {
		if (tapEvent instanceof TapInsertRecordEvent) {
			((TapInsertRecordEvent) tapEvent).setAfter(after);
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			((TapUpdateRecordEvent) tapEvent).setAfter(after);
		}
	}

	public static String getOp(TapEvent tapEvent) {
		String op = "";
		if (tapEvent instanceof TapInsertRecordEvent) {
			op = OperationType.INSERT.getOp();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			op = OperationType.UPDATE.getOp();
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			op = OperationType.DELETE.getOp();
		} else if (tapEvent instanceof TapDDLEvent) {
			op = OperationType.DDL.getOp();
		}
		return op;
	}

	public static String getTableId(TapEvent tapEvent) {
		if (tapEvent instanceof TapRecordEvent) {
			return ((TapRecordEvent) tapEvent).getTableId();
		} else if (tapEvent instanceof TapDDLEvent) {
			return ((TapDDLEvent) tapEvent).getTableId();
		}
		return "";
	}

	public static Long getTimestamp(TapEvent tapEvent) {
		Long timestamp = null;
		if (tapEvent instanceof TapRecordEvent) {
			timestamp = ((TapRecordEvent) tapEvent).getReferenceTime();
		} else if (tapEvent instanceof TapDDLEvent) {
			timestamp = ((TapDDLEvent) tapEvent).getReferenceTime();
		}
		return timestamp;
	}
}
