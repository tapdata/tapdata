package io.tapdata.pdk.tdd.tests.support.printf;

public class SummaryData {
    int totalCase = 0;
    int exeCase = 0;
    int error = 0;
    int succeed = 0;
    int warn = 0;
    int dump = 0;

    int needExecCase = 0;

    public void needOne() {
        this.needExecCase++;
    }

    public void needAny(int num) {
        this.needExecCase += num;
    }

    public int needExecCase(){
        return this.needExecCase;
    }

    public void summaryOnce(int totalCase, int exeCase, int succeed, int warn, int error, int dump) {
        this.totalCase += totalCase;
        this.exeCase += exeCase;
        this.error += error;
        this.succeed += succeed;
        this.warn += warn;
        this.dump += dump;
    }

    public int totalCaseInc() {
        return ++this.totalCase;
    }

    public int exeCaseInc() {
        return ++this.exeCase;
    }

    public int errorInc() {
        return ++this.error;
    }

    public int succeedInc() {
        return ++this.succeed;
    }

    public int warnInc() {
        return ++this.warn;
    }

    public int dumpInc() {
        return ++this.dump;
    }

    public int totalCaseInc(int num) {
        return this.totalCase += num;
    }

    public int exeCaseInc(int num) {
        return this.exeCase += num;
    }

    public int errorInc(int num) {
        return this.error += num;
    }

    public int succeedInc(int num) {
        return this.succeed += num;
    }

    public int warnInc(int num) {
        return this.warn += num;
    }

    public int dumpInc(int num) {
        return this.dump += num;
    }

    public int totalCase() {
        return this.totalCase;
    }

    public int exeCase() {
        return this.exeCase;
    }

    public int error() {
        return this.error;
    }

    public int succeed() {
        return this.succeed;
    }

    public int warn() {
        return this.warn;
    }

    public int dump() {
        return this.dump;
    }
}