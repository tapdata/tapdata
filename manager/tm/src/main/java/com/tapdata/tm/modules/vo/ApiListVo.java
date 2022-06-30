package com.tapdata.tm.modules.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.tm.vo.BaseVo;
import lombok.*;

@Data
@EqualsAndHashCode(callSuper=false)
public class ApiListVo extends BaseVo {
//    @JsonInclude(JsonInclude.Include.ALWAYS)
//    private String apiId;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String name;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String status;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Long visitLine;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Long visitCount;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Long transitQuantity;
}
