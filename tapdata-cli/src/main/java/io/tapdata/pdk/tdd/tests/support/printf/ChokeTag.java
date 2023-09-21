package io.tapdata.pdk.tdd.tests.support.printf;

public class ChokeTag{
    boolean choked;
    public static ChokeTag tag(){
        return new ChokeTag();
    }
    public ChokeTag blocked(){
        this.choked = true;
        return this;
    }
    public boolean hasBlocked(){
        return this.choked ;
    }
}