package io.tapdata.ct.test;

public class Student {

    private int age;
    private String name;

    public int getAge() {
        return age;

    }

    public void setAge(int age) {
        this.age = age;

    }

    public String getName() {
        return name;

    }

    public void setName(String name) {
        this.name = name;

    }

    public void sayHello() {

        System.out.println("Hello, My name is " + this.name);

    }

}