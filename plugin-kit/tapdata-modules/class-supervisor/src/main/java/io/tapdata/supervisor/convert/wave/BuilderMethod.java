package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.JavassistTag;
import javassist.*;
import javassist.bytecode.Descriptor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class BuilderMethod implements Builder<BuilderMethod> {
    CtClass ctClass;
    CtMethod method;

    protected BuilderMethod(CtClass ctClass, String methodName, List<String> args, String returnType, boolean createNotExist, String createWith) throws NotFoundException, CannotCompileException {
        this.ctClass = ctClass;
        ClassPool pool = ctClass.getClassPool();
        CtClass[] ctClasses = CtClassGetter.byName(pool, args);
        CtClass returnClass = CtClassGetter.byName(pool, returnType);
        try {
            //String parameters = Descriptor.ofMethod(returnClass, ctClasses);
            this.method = ctClass.getDeclaredMethod(methodName, ctClasses);
        } catch (NotFoundException e) {
            CtMethod m = new CtMethod(returnClass, methodName, ctClasses, this.ctClass);
            String parameters = Descriptor.ofMethod(returnClass, ctClasses);
            try {
                CtMethod ctMethod = ctClass.getMethod(methodName, parameters);
                try {
                    Class<?>[] argsClass = new Class[args.size()];
                    for (int index = 0; index < args.size() ; index++) {
                        argsClass[index] = Class.forName(args.get(index));
                    }
                    Class<?> classMethod = Class.forName(ctClass.getName());
                    Method method = classMethod.getMethod(methodName, argsClass);
                    int modifiers = method.getModifiers();
                    if (Modifier.isFinal(modifiers)){
                        throw new RuntimeException("Cannot get declared method named is " + methodName + ", and method is final." );
                    }else if (Modifier.isPrivate(modifiers)){
                        throw new RuntimeException("Cannot get declared method named is " + methodName + ", and method is private.");
                    }
                }catch (ClassNotFoundException | SecurityException | NoSuchMethodException foundException){
                    throw new CannotCompileException(foundException.getMessage());
                }
                //m = ctMethod;
                //m.setWrappedBody(ctMethod, null);
            } catch (CannotCompileException ex) {
                if (createNotExist) {
                    m.setModifiers(Modifier.PUBLIC);
                    try {
                        m.setBody(createWith);
                    } catch (Exception es) {
                        m.setBody("{ Object varES;}");
                    }
                } else {
                    throw new CannotCompileException("Cannot get declared method named is " + methodName + " :" + e.getMessage() + ", and " + ex.getMessage());
                }
            }
            this.ctClass.addMethod(m);
            this.method = m;
        }
    }

    //create @todo

    public BuilderMethod appendBefore(String script) throws CannotCompileException {
        this.method.insertBefore(script);
        System.out.printf("[Method %s %s] insert code after successfully%n", this.ctClass.getName(), this.method.getName());
        return this;
    }

    public BuilderMethod appendAfter(boolean needFinally, boolean needRedundant, String script) throws CannotCompileException {
        this.method.insertAfter(script, needFinally);
        System.out.printf("[Method %s %s] insert code after successfully%n", this.ctClass.getName(), this.method.getName());
        return this;
    }

    public BuilderMethod appendAt(int atIndex, boolean needModify, String script) throws CannotCompileException {
        this.method.insertAt(atIndex, needModify, script);
        System.out.printf("[Method %s %s] insert code at %s successfully%n", this.ctClass.getName(), this.method.getName(), atIndex);
        return this;
    }

    @Override
    public BuilderMethod appendCatch(String exception, String name, String script) throws NotFoundException, CannotCompileException {
        ClassPool pool = ctClass.getClassPool();
        if (Objects.isNull(exception) || JavassistTag.EMPTY.equals(exception.trim()))
            exception = JavassistTag.DEFAULT_CATCH_PATH;
        if (Objects.isNull(name) || JavassistTag.EMPTY.equals(name.trim()))
            name = JavassistTag.DEFAULT_CATCH_EXCEPTION_NAME;
        this.method.addCatch(script, pool.get(exception), name);
        System.out.printf("[Method %s %s] insert code catch successfully%n", this.ctClass.getName(), this.method.getName());
        return this;
    }
}