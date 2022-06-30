package com.tapdata.tm.cluster.dto;

import lombok.*;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class AccessNodeInfo {
    private String processId;
    private String hostName;
    private String ip;
}
