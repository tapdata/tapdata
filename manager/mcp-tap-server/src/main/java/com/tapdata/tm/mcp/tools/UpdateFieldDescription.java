package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tapdata.tm.mcp.Utils.*;

/**
 * MCP tool for updating field business description in table model.
 * Supports batch update of multiple fields.
 *
 * @author Feynman
 * create at 2025/5/19
 */
@Slf4j
@Component
public class UpdateFieldDescription extends Tool {

    private final MetadataInstancesService metadataInstancesService;

    public UpdateFieldDescription(SessionAttribute sessionAttribute, MetadataInstancesService metadataInstancesService, UserService userService) {
        super("updateFieldDescription", "Update business description for fields in table model. Supports batch update.",
                readJsonSchema("UpdateFieldDescription.json"), sessionAttribute, userService);
        this.metadataInstancesService = metadataInstancesService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        UserDetail userDetail = getUserDetail(exchange);
        String metadataId = getStringValue(params, "metadataId");
        List<Map<String, Object>> fields = (List<Map<String, Object>>) params.get("fields");

        if (StringUtils.isBlank(metadataId)) {
            throw new RuntimeException("Parameter metadataId is required.");
        }
        if (CollectionUtils.isEmpty(fields)) {
            throw new RuntimeException("Parameter fields is required and cannot be empty.");
        }

        // Separate fields by update method: by fieldId or by fieldName
        List<Map<String, Object>> updatedByFieldId = new ArrayList<>();
        Map<String, String> fieldNameDescMap = new HashMap<>();

        for (Map<String, Object> field : fields) {
            String fieldId = getStringValue(field, "fieldId");
            String fieldName = getStringValue(field, "fieldName");
            String businessDesc = getStringValue(field, "businessDesc");

            if (StringUtils.isBlank(businessDesc)) {
                continue; // skip fields without businessDesc
            }

            if (StringUtils.isNotBlank(fieldId)) {
                // Update by fieldId one by one
                DiscoveryFieldDto dto = new DiscoveryFieldDto();
                dto.setId(fieldId);
                dto.setBusinessDesc(businessDesc);
                metadataInstancesService.updateTableFieldDesc(metadataId, dto, userDetail);
                updatedByFieldId.add(field);
            } else if (StringUtils.isNotBlank(fieldName)) {
                // Collect for batch update by fieldName
                fieldNameDescMap.put(fieldName, businessDesc);
            }
        }

        // Batch update by fieldName
        if (!fieldNameDescMap.isEmpty()) {
            metadataInstancesService.batchUpdateTableFieldDescByName(metadataId, fieldNameDescMap, userDetail);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("metadataId", metadataId);
        result.put("updatedCount", updatedByFieldId.size() + fieldNameDescMap.size());
        result.put("message", "Field descriptions updated successfully");

        return makeCallToolResult(result);
    }
}
