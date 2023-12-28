package com.tapdata.tm.commons.metrics;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class ConnectorRecordDto extends BaseDto {
    private String pdkHash;
    private String status;
    private String downloadSpeed;
    private String downFiledMessage;
    private Boolean flag;
    private String connectionId;
    private Long fileSize;
    private Long progress;

    public enum StatusEnum {
        FINISH("finish"),
        FAIL("fail"),
        DOWNLOADING("downloading")
        ;
        private final String status;

        StatusEnum(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }
}
