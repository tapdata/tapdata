package io.tapdata.aspect.taskmilestones;

import io.tapdata.entity.aspect.Aspect;
import lombok.Getter;


public class EngineDeductionAspect extends Aspect {
    public static final int DEDUCTION_START = 10;
    public static final int DEDUCTION_END = 20;
    public static final int DEDUCTION_ERROR = 30;

    @Getter
    private int state;
    public Long endTime;

    public EngineDeductionAspect start() {
        this.state = DEDUCTION_START;
        return this;
    }

    public EngineDeductionAspect end() {
        this.state = DEDUCTION_END;
        this.endTime = System.currentTimeMillis();
        return this;
    }

    @Getter
    private Throwable error;

    public EngineDeductionAspect error(Throwable error) {
        this.error = error;
        this.state = DEDUCTION_ERROR;
        this.endTime = System.currentTimeMillis();
        return this;
    }

}
