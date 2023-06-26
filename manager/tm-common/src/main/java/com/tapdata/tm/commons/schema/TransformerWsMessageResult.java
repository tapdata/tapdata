package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Message;
import lombok.Data;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;

@Data
public class TransformerWsMessageResult {

    private List<MetadataInstancesDto> batchInsertMetaDataList;
    private List<String> batchRemoveMetaDataList;
    private Map<String, MetadataInstancesDto> batchMetadataUpdateMap;
    private List<MetadataTransformerItemDto> upsertItems;
    private List<MetadataTransformerDto> upsertTransformer;
    private Map<String, List<Message>> transformSchema;
    private String taskId;
    private DAG dag;
    private String transformUuid;
}
