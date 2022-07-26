package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransformerWsMessageDto {

    private TaskDto taskDto;
    private DAG.Options options;
    private List<MetadataInstancesDto> metadataInstancesDtoList;
    private Map<String, DataSourceConnectionDto> dataSourceMap;
    private Map<String, DataSourceDefinitionDto> definitionDtoMap;
    private String userId;
    private String userName;
    private Map<String, MetadataTransformerDto> transformerDtoMap;
    private String type;
}
