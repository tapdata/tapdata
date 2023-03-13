package io.tapdata.ct.dto;

public class ForTest implements Clue{
    @Override
    public void after() {
        System.out.println("System.out.println(\"1: this is hack code! Be careful please!\");");
    }

    @Override
    public void before() {
        System.out.println("System.out.println(\"3: this is hack code! Be careful please!\");");
    }
}
