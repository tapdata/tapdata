package com.tapdata.tm.skiperrortable.vo;

import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import com.tapdata.tm.skiperrortable.dto.TaskSkipErrorTableDto;
import com.tapdata.tm.taskinspect.vo.MapCreator;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * 跳过错误表上报实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/9/2 15:58 Create
 */
@Getter
@Setter
public class SkipErrorTableStatusVo implements Serializable {

    private String sourceTable;
    private SkipErrorTableStatusEnum status;


    public SkipErrorTableStatusVo sourceTable(String sourceTable) {
        setSourceTable(sourceTable);
        return this;
    }

    public SkipErrorTableStatusVo status(SkipErrorTableStatusEnum status) {
        setStatus(status);
        return this;
    }

    public SkipErrorTableStatusVo status(String status) {
        setStatus(SkipErrorTableStatusEnum.parse(status));
        return this;
    }

    public Map<String, Object> toMap() {
        return MapCreator.<String, Object>create(TaskSkipErrorTableDto.FIELD_SOURCE_TABLE, getSourceTable())
            .add(TaskSkipErrorTableDto.FIELD_STATUS, getStatus());
    }

    public static SkipErrorTableStatusVo create() {
        return new SkipErrorTableStatusVo();
    }
}
