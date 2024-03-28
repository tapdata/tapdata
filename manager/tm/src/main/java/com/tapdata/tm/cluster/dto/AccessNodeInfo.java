package com.tapdata.tm.cluster.dto;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.util.List;

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

    public AccessNodeInfo(String processId, String hostName, String ip, String status) {
        this.processId = processId;
        this.hostName = hostName;
        this.ip = ip;
        this.status = status;
    }

    /**
     * 访问节点, 默认为 MANUALLY_SPECIFIED_BY_THE_USER
     * 类型 默认为“平台自动分配”可选择“用户手动指定” --AccessNodeTypeEnum
     */
    private String accessNodeName;
    private String accessNodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name();
    /**
     * 每个分组标签包含多个节点信息，普通节点则不包含
     * */
    private List<AccessNodeInfo> accessNodes;
}
