package com.tapdata.tm.dql.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * Internal Engine-facing view of the immutable payload stored with one DQL event.
 * It contains the immutable payload and the DAG node metadata required by
 * Engine to replay the event from the failed node.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlRecoveryPayloadVo implements Serializable {
    private String sourceNodeId;
    private String sourceNodeName;
    private String failedNodeId;
    private String failedNodeName;
    private String targetNodeId;
    private String targetNodeName;
    private String payloadFormat;
    private Object payloadData;
    private String payloadHash;
    private Long payloadSize;
    private Boolean payloadComplete;
    private Map<String, Object> payloadPreview;
    private Boolean payloadPreviewTruncated;
}
