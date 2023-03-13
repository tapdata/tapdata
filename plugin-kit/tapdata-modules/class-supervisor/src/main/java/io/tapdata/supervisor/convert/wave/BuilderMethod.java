package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.JavassistTag;
import javassist.*;
import javassist.bytecode.Descriptor;

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
            String parameters = Descriptor.ofMethod(returnClass, ctClasses);
            this.method = ctClass.getMethod(methodName, parameters);
        } catch (NotFoundException e) {
            if (createNotExist) {
                CtMethod m = new CtMethod(returnClass, methodName, ctClasses, this.ctClass);
                m.setModifiers(Modifier.PUBLIC);
                m.setBody(createWith);
                this.ctClass.addMethod(m);
                this.method = m;
            } else {
                throw e;
            }
        }
    }

    //create @todo

    public BuilderMethod appendBefore(String script) throws CannotCompileException {
        this.method.insertBefore(script);
        return this;
    }

    public BuilderMethod appendAfter(boolean needFinally, boolean needRedundant, String script) throws CannotCompileException {
        this.method.insertAfter(script, needFinally, needRedundant);
        return this;
    }

    public BuilderMethod appendAt(int atIndex, boolean needModify, String script) throws CannotCompileException {
        this.method.insertAt(atIndex, needModify, script);
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
        return this;
    }
}