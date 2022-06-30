package io.tapdata.debug;

import java.util.List;
import java.util.Map;

public interface Debug {

	void clearDebugData(String dataFlowId) throws DebugException;

	void clearDebugLogs(String dataFlowId) throws DebugException;

	void clearGridfsFiles(String dataFlowId) throws DebugException;

	void writeDebugData(List<Map<String, Object>> datas) throws DebugException;
}
