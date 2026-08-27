package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.Map;

/**
 * Serializable snapshot fields required to persist and later replay one TapRecordEvent.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlPayloadSnapshot {
    private String payloadFormat;
    private Object payloadData;
    private String payloadHash;
    private Long payloadSize;
    private Boolean payloadComplete;
    private Map<String, Object> payloadPreview;
    private Boolean payloadPreviewTruncated;
}
