package io.tapdata.connector.doris.streamload.rest.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author dayun
 * @Date 7/14/22
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Setter
@Getter
public class RespContent {

    @JsonProperty(value = "TxnId")
    private long TxnId;

    @JsonProperty(value = "Label")
    private String Label;

    @JsonProperty(value = "Status")
    private String Status;

    @JsonProperty(value = "TwoPhaseCommit")
    private String TwoPhaseCommit;

    @JsonProperty(value = "ExistingJobStatus")
    private String ExistingJobStatus;

    @JsonProperty(value = "Message")
    private String Message;

    @JsonProperty(value = "NumberTotalRows")
    private long NumberTotalRows;

    @JsonProperty(value = "NumberLoadedRows")
    private long NumberLoadedRows;

    @JsonProperty(value = "NumberFilteredRows")
    private int NumberFilteredRows;

    @JsonProperty(value = "NumberUnselectedRows")
    private int NumberUnselectedRows;

    @JsonProperty(value = "LoadBytes")
    private long LoadBytes;

    @JsonProperty(value = "LoadTimeMs")
    private int LoadTimeMs;

    @JsonProperty(value = "BeginTxnTimeMs")
    private int BeginTxnTimeMs;

    @JsonProperty(value = "StreamLoadPutTimeMs")
    private int StreamLoadPutTimeMs;

    @JsonProperty(value = "ReadDataTimeMs")
    private int ReadDataTimeMs;

    @JsonProperty(value = "WriteDataTimeMs")
    private int WriteDataTimeMs;

    @JsonProperty(value = "CommitAndPublishTimeMs")
    private int CommitAndPublishTimeMs;

    @JsonProperty(value = "ErrorURL")
    private String ErrorURL;

    public long getTxnId() {
        return TxnId;
    }

    public String getStatus() {
        return Status;
    }

    public String getTwoPhaseCommit() {
        return TwoPhaseCommit;
    }

    public String getMessage() {
        return Message;
    }

    public String getExistingJobStatus() {
        return ExistingJobStatus;
    }
    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }

    }

    public String getErrorURL() {
        return ErrorURL;
    }

    public boolean isSuccess() {
        return "Success".equals(this.Status);
    }
}
