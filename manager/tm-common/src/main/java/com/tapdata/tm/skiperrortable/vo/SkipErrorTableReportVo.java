package com.tapdata.tm.skiperrortable.vo;

import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.taskinspect.vo.MapCreator;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 跳过错误表上报实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/9/2 15:58 Create
 */
@Getter
@Setter
public class SkipErrorTableReportVo {

    private String sourceTableName;
    private String targetTableName;
    private String skipStage;
    private Long skipDate;
    private Long cdcDate;
    private String errorCode;
    private String errorMessage;

    public SkipErrorTableReportVo sourceTableName(String sourceTableName) {
        setSourceTableName(sourceTableName);
        return this;
    }

    public SkipErrorTableReportVo targetTableName(String targetTableName) {
        setTargetTableName(targetTableName);
        return this;
    }

    public SkipErrorTableReportVo skipStage(String skipStage) {
        setSkipStage(skipStage);
        return this;
    }

    public SkipErrorTableReportVo cdcDate(Long cdcDate) {
        setCdcDate(cdcDate);
        return this;
    }

    public SkipErrorTableReportVo errorCode(String errorCode) {
        setErrorCode(errorCode);
        return this;
    }

    public SkipErrorTableReportVo errorMessage(String errorMessage) {
        setErrorMessage(errorMessage);
        return this;
    }

    public Map<String, Object> toMap() {
        return MapCreator.<String, Object>create(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE, getSourceTableName())
            .add(TaskSkipErrorTableDto.FIELD_TARGET_TABLE, getTargetTableName())
            .add(TaskSkipErrorTableDto.FIELD_SKIP_STAGE, getSkipStage())
            .add(TaskSkipErrorTableDto.FIELD_SKIP_DATE, getSkipDate())
            .add(TaskSkipErrorTableDto.FIELD_CDC_DATE, getCdcDate())
            .add(TaskSkipErrorTableDto.FIELD_ERROR_CODE, getErrorCode())
            .add(TaskSkipErrorTableDto.FIELD_ERROR_MESSAGE, getErrorMessage());
    }

    public static SkipErrorTableReportVo create() {
        SkipErrorTableReportVo vo = new SkipErrorTableReportVo();
        vo.setSkipDate(System.currentTimeMillis());
        return vo;
    }
}
