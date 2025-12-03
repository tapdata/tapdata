package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase;

import lombok.Data;

@Data
public class JudgeResult {
    int type;
    double rate;
    String reason;
    String detail;

    boolean completed;
    boolean hasJudge;

    public int type() {
        return type;
    }

    public double rate() {
        return rate;
    }

    public String reason() {
        return reason;
    }

    public String detail() {
        return detail;
    }

    public void type(int type) {
       this.setType(type);
    }

    public void rate(double rate) {
        this.setRate(rate);
    }

    public void reason(String reason) {
        this.reason = reason;
    }

    public void detail(String detail) {
        this.detail = detail;
    }
}