package io.tapdata.ct;

import io.tapdata.ct.test.ClassUtil;
import javassist.*;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class CtTClass {
    @Test
    public void main1() throws NotFoundException, CannotCompileException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        ClassPool pool = ClassPool.getDefault();
        CtClass thread = pool.get("java.lang.Thread");
        CtMethod run = thread.getDeclaredMethod("run");
        run.insertBefore("System.out.println(\"1: this is hack code! Be careful please!\");");
        run.insertAt(2, "System.out.println(\"2: this is hack code! Be careful please!\");");
        run.insertAfter("System.out.println(\"3: this is hack code! Be careful please!\");");
        thread.writeFile();
        CtClass clas = pool.get("io.tapdata.ct.dto");
        CtMethod before = thread.getDeclaredMethod("before");
        CtMethod after = thread.getDeclaredMethod("after");
    }
}
