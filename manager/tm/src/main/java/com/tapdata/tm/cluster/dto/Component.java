package com.tapdata.tm.cluster.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/13 下午9:59
 * @description
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class Component {
    @Schema(description = "进程状态")
    private String status;
    private String processID;
    @Schema(description = "服务状态")
    private String serviceStatus;
}
