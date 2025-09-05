package com.tapdata.tm.apiCalls.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 21:32 Create
 * @description
 */

@EqualsAndHashCode(callSuper = true)
@Data
@Document("WorkerCallStats")
public class WorkerCallStats extends BaseEntity {
    private String allPathId;
    private String workOid;
    private String processId;
    private long totalCount;
    private long notOkCount;
    private String lastCallId;
    private Boolean delete;
}
