package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MCP tool for updating field business description in table model.
 * Supports batch update of multiple fields.
 *
 * @author Feynman
 * create at 2025/5/19
 */
@Slf4j
@Component
public class UpdateFieldDescription {

    private final McpToolSupport toolSupport;
    private final MetadataInstancesService metadataInstancesService;

    public UpdateFieldDescription(McpToolSupport toolSupport, MetadataInstancesService metadataInstancesService) {
        this.toolSupport = toolSupport;
        this.metadataInstancesService = metadataInstancesService;
    }

    @McpTool(name = "updateFieldDescription", description = "Update business descriptions for fields in a table model. Supports batch update.")
    public Map<String, Object> updateFieldDescription(
            McpSyncRequestContext context,
            @McpToolParam(description = "Metadata instance id of the table model.") String metadataId,
            @McpToolParam(description = "Field description updates. Use either fieldId or fieldName to identify each field.") List<FieldDescriptionUpdate> fields) {
        UserDetail userDetail = toolSupport.getUserDetail(context);

        if (StringUtils.isBlank(metadataId)) {
            throw new RuntimeException("Parameter metadataId is required.");
        }
        if (CollectionUtils.isEmpty(fields)) {
            throw new RuntimeException("Parameter fields is required and cannot be empty.");
        }

        // Separate fields by update method: by fieldId or by fieldName
        List<FieldDescriptionUpdate> updatedByFieldId = new ArrayList<>();
        Map<String, String> fieldNameDescMap = new HashMap<>();

        for (FieldDescriptionUpdate field : fields) {
            if (field == null) {
                continue;
            }
            String fieldId = field.fieldId;
            String fieldName = field.fieldName;
            String businessDesc = field.businessDesc;

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

        return result;
    }

    public static class FieldDescriptionUpdate {
        @McpToolParam(required = false, description = "Field id. Preferred when available.")
        public String fieldId;

        @McpToolParam(required = false, description = "Field name. Used when fieldId is not available.")
        public String fieldName;

        @McpToolParam(description = "Business description to set on the field.")
        public String businessDesc;
    }
}
