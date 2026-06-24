package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
@Component
public class ListDataModel {

    private final McpToolSupport toolSupport;
    private final MetadataInstancesService metadataInstancesService;

    public ListDataModel(McpToolSupport toolSupport, MetadataInstancesService metadataInstancesService) {
        this.toolSupport = toolSupport;
        this.metadataInstancesService = metadataInstancesService;
    }

    @McpTool(name = "listDataModel", description = "List data models loaded by a TapData connection")
    public List<Map<String, Object>> listDataModel(
            McpSyncRequestContext context,
            @McpToolParam(description = "TapData connection id.") String connectionId,
            @McpToolParam(required = false, description = "Whether to include fields and indexes in each data model.") Boolean includeFields,
            @McpToolParam(required = false, description = "Optional case-insensitive table or collection name keyword.") String name) {
        UserDetail userDetail = toolSupport.getUserDetail(context);
        boolean fieldsIncluded = Boolean.TRUE.equals(includeFields);

        if (StringUtils.isBlank(connectionId))
            throw new RuntimeException("Parameter connectionId is required.");

        // Build filter with where conditions
        Filter filter = new Filter();
        Where where = filter.getWhere();
        where.put("source.id", connectionId);
        where.put("is_deleted", false);
        where.put("sourceType", SourceTypeEnum.SOURCE.name());

        // If tableName is provided, filter by original_name to save time
        if (StringUtils.isNotBlank(name)) {
            Map<String, Object> likeCondition = new HashMap<>();
            likeCondition.put("$regex", name);
            likeCondition.put("$options", "i");
            where.put("original_name", likeCondition);
        }

        // Set fields projection
        Field fields = new Field();
        fields.put("original_name", true);
        fields.put("meta_type", true);
        fields.put("id", true);
        if (fieldsIncluded) {
            fields.put("fields", true);
            fields.put("indices", true);
        }
        filter.setFields(fields);
        filter.setLimit(0); // no limit

        Page<MetadataInstancesDto> page = metadataInstancesService.list(filter, userDetail);
        List<MetadataInstancesDto> metadataList = page.getItems();

        List<Map<String, Object>> result = metadataList.stream().map(metadata -> {
            Map<String, Object> data = new HashMap<>();
            data.put("id", metadata.getId() != null ? metadata.getId().toHexString() : null);
            data.put("type", metadata.getMetaType());
            data.put("name", metadata.getOriginalName());
            data.put("collectionName", metadata.getOriginalName());
            if (fieldsIncluded && CollectionUtils.isNotEmpty(metadata.getFields())) {
                data.put("fields", metadata.getFields().stream().map(field -> {
                    Map<String, Object> f = new HashMap<>();
                    f.put("name", field.getFieldName());
                    f.put("type", field.getDataType());
                    f.put("unique", field.isUnique());
                    f.put("primaryKey", field.getPrimaryKey());
                    return f;
                }).collect(Collectors.toList()));
                if (CollectionUtils.isNotEmpty(metadata.getIndices())) {
                    data.put("indexes", metadata.getIndices().stream().map(index -> {
                        Map<String, Object> f = new HashMap<>();
                        f.put("name", index.getIndexName());
                        f.put("unique", index.isUnique());
                        if (CollectionUtils.isNotEmpty(index.getColumns())) {
                            f.put("keys", index.getColumns().stream()
                                    .map(col -> col.getColumnName())
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList()));
                        } else {
                            f.put("keys", Collections.emptyList());
                        }
                        return f;
                    }).collect(Collectors.toList()));
                } else {
                    data.put("indexes", Collections.emptyList());
                }
            }
            return data;
        }).collect(Collectors.toList());

        return result;
    }
}
