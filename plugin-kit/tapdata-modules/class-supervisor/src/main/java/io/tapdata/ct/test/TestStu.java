package io.tapdata.ct.test;

import javassist.*;
import org.junit.Test;

import java.lang.reflect.Method;

public class TestStu {
    @Test
    public void t3() throws Exception {
        ClassPool pool = ClassPool.getDefault();
        CtClass teacherClass = pool.makeClass("io.tapdata.ct.test.Teacher");
        CtClass stringClass = pool.get("java.lang.String");
        CtClass listClass = pool.get("java.util.List");
        CtClass studentClass = pool.get("io.tapdata.ct.test.Student");

        CtField nameField = new CtField(stringClass, "name", teacherClass);
        nameField.setModifiers(Modifier.PUBLIC);
        teacherClass.addField(nameField);
        CtField studentList = new CtField(listClass, "students", teacherClass);
        studentList.setModifiers(Modifier.PUBLIC);
        teacherClass.addField(studentList);

        CtConstructor ctConstructor = CtNewConstructor.make("public Teacher(){this.name=\"abc\";this.students = new java.util.ArrayList();}", teacherClass);
        teacherClass.addConstructor(ctConstructor);

        CtMethod m = new CtMethod(CtClass.voidType, "addStudent", new CtClass[]{studentClass}, teacherClass);
        m.setModifiers(Modifier.PUBLIC);
        m.setBody("this.students.add($1);");
        teacherClass.addMethod(m);

        m = new CtMethod(CtClass.voidType, "sayHello", new CtClass[]{}, teacherClass);
        m.setModifiers(Modifier.PUBLIC);
        m.setBody("System.out.println(\"Hello, My name is \" + this.name);");
        m.insertAfter("System.out.println(\"I have \" + this.students.size() + \" students\");");
        teacherClass.addMethod(m);
        Class<?> cls = teacherClass.toClass();
        Object obj = cls.newInstance();
        Method method = cls.getDeclaredMethod("addStudent", Student.class);

        Student stu = new Student();
        stu.setName("张三");
        method.invoke(obj, stu);
        stu = new Student();
        stu.setName("李四");

        method.invoke(obj, stu);

        Method teacherSayHello = cls.getDeclaredMethod("sayHello", new Class[]{});
        teacherSayHello.invoke(obj, new Object[]{});
        stringClass.writeFile();
        teacherClass.writeFile("io.tapdata.ct.test");
    }
}
