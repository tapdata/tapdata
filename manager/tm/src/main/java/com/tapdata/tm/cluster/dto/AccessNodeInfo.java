package com.tapdata.tm.cluster.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class AccessNodeInfo {
    /**
     * 如果accessNodeType 是MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，accessNodeProcessId表示Agent分组的groupId
     * */
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
     * 访问节点
     * 类型 默认为“平台自动分配”可选择“用户手动指定” --AccessNodeTypeEnum
     * 如果accessNodeType 是MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，accessNodeName表示Agent分组的group name
     */
    private String accessNodeName;
    private String accessNodeType;
    /**
     * 每个分组标签包含多个节点信息，普通节点则不包含
     * */
    private List<AccessNodeInfo> accessNodes;
}
