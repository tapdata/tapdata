package com.tapdata.tm.cluster.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class AccessNodeInfo {
    private String processId;
    private String hostName;
    private String ip;
    @Schema(description = "引擎运行状态")
    private String status;
}
