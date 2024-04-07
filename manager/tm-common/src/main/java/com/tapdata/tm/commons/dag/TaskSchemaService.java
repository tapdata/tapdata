package com.tapdata.tm.commons.dag;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
@Service
public interface TaskSchemaService {
    public Map<String, Object> discoverSchemaMQ(Node node, Map<String, Object> nodeConfig, String taskId, List<String> tableName);
}
