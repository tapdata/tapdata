package com.tapdata.tm.metadatainstance.bean;

import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class NodeInfoPage {
    private long total;
    private List<MetadataInstancesDto> items;
    @Schema(description = "全部表的数量")
    private long wholeNum;
    @Schema(description = "更新条件异常的数量")
    private long updateExNum;
    @Schema(description = "推演异常的数量")
    private long transformExNum;
}
