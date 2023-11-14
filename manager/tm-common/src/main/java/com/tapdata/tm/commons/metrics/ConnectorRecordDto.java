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

    public enum statusEnum {
        FINISH("finish"),
        FAIL("fail")
        ;
        private final String status;

        statusEnum(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }
}
