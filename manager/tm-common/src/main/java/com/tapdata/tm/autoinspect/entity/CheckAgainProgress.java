package com.tapdata.tm.autoinspect.entity;

import com.tapdata.tm.autoinspect.constants.CheckAgainStatus;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Date;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/17 10:51 Create
 */
@Data
public class CheckAgainProgress implements Serializable {
    private @NonNull CheckAgainStatus status; //再次校验状态
    private @NonNull String sn; //批处序号
    private @NonNull Date beginAt; //开始时间
    private Date completedAt; //完成时间
    private @NonNull Date updated; //最后更新时间
    private String msg; //提示信息
    private long checkedCounts; //已校验数量
    private long fixCounts; //已消除数量

    public CheckAgainProgress() {
    }

    public CheckAgainProgress(@NonNull String sn) {
        this.sn = sn;
        this.status = CheckAgainStatus.Scheduling;
        this.beginAt = new Date();
        this.updated = new Date();
    }

    public long useTimes() {
        if (null != completedAt) {
            return completedAt.getTime() - beginAt.getTime();
        }
        return 0;
    }
}


