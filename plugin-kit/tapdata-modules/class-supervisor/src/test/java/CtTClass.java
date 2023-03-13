import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.supervisor.convert.entity.Parser;
import javassist.*;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class CtTClass {
    ClassPool pool = ClassPool.getDefault();
    @Test
    public void main1() throws NotFoundException, CannotCompileException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        ClassPool pool = ClassPool.getDefault();
        CtClass thread = pool.get("java.lang.Thread");
        CtMethod run = thread.getDeclaredMethod("run");
        run.insertAfter("System.out.println(\"3: this is hack code! Be careful please!\");", true, false);
        run.insertBefore("System.out.println(\"1: this is hack code! Be careful please!\");");
        run.insertAt(2, "System.out.println(\"2: this is hack code! Be careful please!\");");
        CtClass throwAble = pool.get("java.lang.Throwable");
        run.addCatch("System.out.println(\"0: this is hack code! Be careful please!\" + $e.getMessage());throw $e;", throwAble);
        thread.writeFile();

//        Class<?> aClass = thread.toClass();
//        Object o = aClass.newInstance();
//
//        Method method = aClass.getMethod("run");
//        method.invoke(o);

        new Thread(()->{
            System.out.println("Hello!");
        }).start();

//        CtClass clas = pool.get("io.tapdata.ct.dto");
//        CtMethod before = thread.getDeclaredMethod("before");
//        CtMethod after = thread.getDeclaredMethod("after");
    }

    @Test
    public void close() throws Exception {

        CtClass thread = pool.get("io.tapdata.ct.thread.ThreadParent");
        pool.get("int");
        CtMethod run = thread.getDeclaredMethod("close");
        run.insertAfter("System.out.println(\"3: this is hack code! Be careful please!\");", true, false);
        run.insertBefore("System.out.println(\"1: this is hack code! Be careful please!\");");
        run.insertAt(2, "System.out.println(\"2: this is hack code! Be careful please!\");");
        CtClass throwAble = pool.get("java.lang.Throwable");
        run.addCatch("System.out.println(\"0: this is hack code! Be careful please!\" + $e.getMessage());throw $e;", throwAble);
        thread.writeFile();

        CtClass[] p = new CtClass[]{
                pool.get(""),
                pool.get("")
        };
        CtConstructor c = new CtConstructor(p, thread);

        thread.addConstructor(c);

        Class<?> aClass = thread.toClass();
        Object o = aClass.newInstance();

        Method method = aClass.getMethod("close");
        method.invoke(o);
    }

    @Test
    public void newCon() throws Exception {
        CtClass intCls = pool.get("int");
        CtClass thread = pool.get("io.tapdata.ct.thread.ThreadParent");
        CtClass obj = pool.get("java.lang.Object");
        CtClass exception = pool.get("java.lang.Exception");
        CtConstructor constructor = CtNewConstructor.make(new CtClass[]{
                intCls,
                obj
        },new CtClass[]{
                exception
        },thread);
        constructor.setBody("{int i=0;i++;System.out.println(i);}");
        //constructor.addLocalVariable("i",intCls);
        //constructor.insertBefore("i = 0;");
        //constructor.insertAfter("i++;");
        //constructor.insertAt(0,"System.out.println(i);");
    }

    @Test
    public void ttt(){
        Parser parser = Parser.parser(JSONUtil.parseObj(json));
        try {
            parser.wave();
        } catch (NotFoundException | CannotCompileException exception) {
            exception.printStackTrace();
        }
    }

    final String json = "{\n" +
            "  \"class\": [\n" +
            "    {\n" +
            "      \"target\": [{\n" +
            "        \"type\": \"extends\",\n" +
            "        \"path\": \"java.io.Closeable\",\n" +
            "        \"scanPackage\": \"*\",\n" +
            "        \"isCreate\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"path\",\n" +
            "        \"path\": \"java.lang.Thread\",\n" +
            "        \"isCreate\": false,\n" +
            "        \"ignore\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"name\",\n" +
            "        \"scanPackage\": \"java.lang\",\n" +
            "        \"path\": \"%Thread%\",\n" +
            "        \"isCreate\": false,\n" +
            "        \"saveTo\": \"java.lang\",\n" +
            "        \"ignore\": true\n" +
            "      },\n" +
            "        {\n" +
            "        \"type\": \"name\",\n" +
            "        \"path\": \"%Thread_\",\n" +
            "        \"isCreate\": false,\n" +
            "        \"saveTo\": \"java.lang\",\n" +
            "          \"ignore\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"name\",\n" +
            "        \"path\": \"_Thread_\",\n" +
            "        \"isCreate\": false,\n" +
            "        \"saveTo\": \"java.lang\",\n" +
            "          \"ignore\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"name\",\n" +
            "        \"path\": \"Thread%\",\n" +
            "        \"isCreate\": false,\n" +
            "        \"saveTo\": \"java.lang\",\n" +
            "          \"ignore\": true\n" +
            "      }\n" +
            "      ],\n" +
            "      \"method\": [\n" +
            "        {\n" +
            "          \"ignore\": true,\n" +
            "          \"isCreate\": false,\n" +
            "          \"name\": \"run\",\n" +
            "          \"args:\": [],\n" +
            "          \"returnType\": \"void\",\n" +
            "          \"code\": [\n" +
            "            {\n" +
            "              \"type\": \"normal\",\n" +
            "              \"index\": 1,\n" +
            "              \"isAppend\": true,\n" +
            "              \"line\": \"System.out.println(\\\"2: this is hack code! Be careful please!\\\");\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"type\": \"before\",\n" +
            "              \"line\": \"System.out.println(\\\"1: this is hack code! Be careful please!\\\");\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"type\": \"after\",\n" +
            "              \"line\": \"System.out.println(\\\"3: this is hack code! Be careful please!\\\");\",\n" +
            "              \"isFinally\": true,\n" +
            "              \"isRedundant\": false\n" +
            "            },\n" +
            "            {\n" +
            "              \"type\": \"catch\",\n" +
            "              \"line\": \"System.out.println(\\\"0: this is hack code! Be careful please!\\\" + $e.getMessage());throw $e;\",\n" +
            "              \"exception\": {\n" +
            "                \"path\": \"java.lang.Throwable\",\n" +
            "                \"name\": \"$e\"\n" +
            "              }\n" +
            "            }\n" +
            "          ]\n" +
            "        },\n" +
            "        {\n" +
            "          \"ignore\": false,\n" +
            "          \"isCreate\": true,\n" +
            "          \"createWith\": \"{super.close();}\"," +
            "          \"name\": \"close\",\n" +
            "          \"args:\": [],\n" +
            "          \"returnType\": \"void\",\n" +
            "          \"code\": [\n" +
            "            {\n" +
            "              \"type\": \"before\",\n" +
            "              \"line\": \"System.out.println(\\\"1: this is hack code! Be careful please!\\\");\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"type\": \"after\",\n" +
            "              \"line\": \"System.out.println(\\\"3: this is hack code! Be careful please!\\\");\",\n" +
            "              \"isFinally\": true,\n" +
            "              \"isRedundant\": false\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      ],\n" +
            "      \"constructor\": [\n" +
            "        {\n" +
            "          \"ignore\": false,\n" +
            "          \"isCreate\": false,\n" +
            "          \"args\": \"*\"," +
            "          \"type\": [\"public\",\"static\",\"void\"],\n" +
            "          \"returnType\": \"void\",\n" +
            "          \"code\": [\n" +
            "            {\n" +
            "              \"type\": \"before\",\n" +
            "              \"line\": \"System.out.println(\\\"1: this is hack code! Be careful please!\\\");\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"type\": \"after\",\n" +
            "              \"line\": \"System.out.println(\\\"3: this is hack code! Be careful please!\\\");\",\n" +
            "              \"isFinally\": true,\n" +
            "              \"isRedundant\": false\n" +
            "            }\n" +
            "          ]\n" +
            "        },\n" +
            "        {\n" +
            "          \"ignore\": false,\n" +
            "          \"isCreate\": false,\n" +
            "          \"args\": [\n" +
            "          ],\n" +
            "          \"type\": [\"public\"],\n" +
            "          \"returnType\": \"void\",\n" +
            "          \"code\": [\n" +
            "            {\n" +
            "              \"type\": \"before\",\n" +
            "              \"line\": \"System.out.println(\\\"1: this is hack code! Be careful please!\\\");\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"type\": \"after\",\n" +
            "              \"line\": \"System.out.println(\\\"3: this is hack code! Be careful please!\\\");\",\n" +
            "              \"isFinally\": true,\n" +
            "              \"isRedundant\": false\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";
}
