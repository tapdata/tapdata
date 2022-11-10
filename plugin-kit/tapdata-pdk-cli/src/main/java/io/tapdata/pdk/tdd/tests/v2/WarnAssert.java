package io.tapdata.pdk.tdd.tests.v2;

public class WarnAssert extends TapAssert{
    public static WarnAssert create(){
        return new WarnAssert();
    }
    public WarnAssert() {
        super(TapAssert.WARN);
    }
}
