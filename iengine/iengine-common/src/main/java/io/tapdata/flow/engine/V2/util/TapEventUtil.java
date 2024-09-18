package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.OperationType;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	public static Map<String, List<String>> getIllegalField(TapEvent tapEvent) {
		Map<String, List<String>> map = new HashMap<>();
		if (tapEvent instanceof TapInsertRecordEvent) {
			map.put("after",((TapInsertRecordEvent) tapEvent).getAfterIllegalDateFieldName());
			return map;
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			map.put("before",((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName());
			map.put("after",((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName());
			return map;
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			map.put("before",((TapDeleteRecordEvent) tapEvent).getBeforeIllegalDateFieldName());
			return map;
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
		if (tapEvent instanceof TapBaseEvent) {
			return ((TapBaseEvent) tapEvent).getTableId();
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

	public static List<String> getNamespaces(TapEvent tapEvent) {
		if (tapEvent instanceof TapBaseEvent) {
			return ((TapBaseEvent) tapEvent).getNamespaces();
		}
		return null;
	}

	public static List<String> getRemoveFields(TapEvent tapEvent) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getRemovedFields();
		} else if (tapEvent instanceof TapInsertRecordEvent) {
			return ((TapInsertRecordEvent) tapEvent).getRemovedFields();
		}
		return null;
	}

	public static void setRemoveFields(TapEvent tapEvent, List<String> removeFields) {
		if (null == tapEvent) {
			return;
		}
		if (tapEvent instanceof TapInsertRecordEvent) {
			((TapInsertRecordEvent) tapEvent).setRemovedFields(removeFields);
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			((TapUpdateRecordEvent) tapEvent).setRemovedFields(removeFields);
		}
	}

	public static Boolean getIsReplaceEvent(TapEvent tapEvent) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getIsReplaceEvent();
		}
		return false;
	}

	public static void setContainsIllegalDate(TapEvent tapEvent, boolean containsIllegalDate) {
		if (tapEvent instanceof TapRecordEvent){
			((TapRecordEvent) tapEvent).setContainsIllegalDate(containsIllegalDate);
		}
	}

	public static void addBeforeIllegalDateField(TapEvent tapEvent, String fieldName) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			if (((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName() == null) {
				((TapUpdateRecordEvent) tapEvent).setBeforeIllegalDateFieldName(new ArrayList<>());
			}
			((TapUpdateRecordEvent) tapEvent).getBeforeIllegalDateFieldName().add(fieldName);
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			if (((TapDeleteRecordEvent) tapEvent).getBeforeIllegalDateFieldName() == null) {
				((TapDeleteRecordEvent) tapEvent).setBeforeIllegalDateFieldName(new ArrayList<>());
			}
			((TapDeleteRecordEvent) tapEvent).getBeforeIllegalDateFieldName().add(fieldName);
		}
	}

	public static void addAfterIllegalDateField(TapEvent tapEvent, String fieldName) {
		if (tapEvent instanceof TapInsertRecordEvent) {
			if (((TapInsertRecordEvent) tapEvent).getAfterIllegalDateFieldName() == null) {
				((TapInsertRecordEvent) tapEvent).setAfterIllegalDateFieldName(new ArrayList<>());
			}
			((TapInsertRecordEvent) tapEvent).getAfterIllegalDateFieldName().add(fieldName);
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			if (((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName() == null) {
				((TapUpdateRecordEvent) tapEvent).setAfterIllegalDateFieldName(new ArrayList<>());
			}
			((TapUpdateRecordEvent) tapEvent).getAfterIllegalDateFieldName().add(fieldName);
		}
	}
}
