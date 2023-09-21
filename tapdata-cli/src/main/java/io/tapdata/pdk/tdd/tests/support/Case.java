package io.tapdata.pdk.tdd.tests.support;

import java.util.ArrayList;
import java.util.List;

public class Case extends History {
    List<History> histories = new ArrayList<>();
    int executionTimes = 0;

    public Case(String tag, String msg) {
        super(tag, msg);
    }

    public static Case create(String tag, String msg) {
        return new Case(tag, msg);
    }

    public List<History> histories() {
        if (null == histories) histories = new ArrayList<>();
        return this.histories;
    }

    public Case addHistory(History history) {
        if (null == histories) histories = new ArrayList<>();
        histories.add(history);
        setTag(history.tag());
        executionTimes++;
        return this;
    }

    public Case addWarn(String message) {
        addHistory(History.warn(message));

        return this;
    }

    public Case addError(String message) {
        addHistory(History.error(message));
        return this;
    }

    public Case addSucceed(String message) {
        addHistory(History.succeed(message));
        return this;
    }

    public Case addTimes() {
        this.executionTimes++;
        return this;
    }

    public Case addTimes(int executionTimes) {
        this.executionTimes += executionTimes;
        return this;
    }

    public int executionTimes() {
        return this.executionTimes;
    }

    private void setTag(String tag) {
        if ((Case.SUCCEED.equals(this.tag) && (Case.WARN.equals(tag) || Case.ERROR.equals(tag)))
                || (Case.WARN.equals(this.tag) && Case.ERROR.equals(tag))) {
            this.tag = tag;
        }
    }
}
