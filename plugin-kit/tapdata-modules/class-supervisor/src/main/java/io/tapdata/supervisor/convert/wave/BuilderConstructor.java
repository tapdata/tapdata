package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.JavassistTag;
import javassist.*;
import javassist.bytecode.AccessFlag;
import javassist.bytecode.Descriptor;

import java.util.List;
import java.util.Objects;

class BuilderConstructor implements Builder<BuilderConstructor> {
    private final CtClass ctClass;
    private final CtConstructor constructor;

    protected BuilderConstructor(CtClass ctClass, List<String> args, String returnType) throws NotFoundException {
        this.ctClass = ctClass;
        ClassPool pool = ctClass.getClassPool();
        CtClass[] ctClasses = CtClassGetter.byName(pool, args);
        String parameters = Descriptor.ofConstructor(ctClasses);
        this.constructor = ctClass.getConstructor(parameters);
    }

    protected BuilderConstructor(CtClass ctClass, CtConstructor constructor) {
        this.constructor = constructor;
        this.ctClass = ctClass;
    }

    protected BuilderConstructor(CtClass ctClass, List<String> args, List<String> exceptions, String returnType, List<String> methodType, String bodyCode) throws NotFoundException, CannotCompileException {
        this.ctClass = ctClass;
        ClassPool pool = this.ctClass.getClassPool();
        CtClass[] params = CtClassGetter.byName(pool, args);
        CtClass[] exceptionsCls = CtClassGetter.byName(pool, exceptions);
        this.constructor = CtNewConstructor.make(params, exceptionsCls, ctClass);
        if (Objects.nonNull(methodType) && !methodType.isEmpty()) {
            for (String mType : methodType) {
                this.constructor.setModifiers(AccessFlag.of(Type.valueIs(mType)));
                this.constructor.getMethodInfo();
            }
        }
        this.constructor.setBody(bodyCode);
        this.ctClass.addConstructor(constructor);
    }

    public BuilderConstructor appendBefore(String script) throws CannotCompileException {
        this.constructor.insertBefore(script);
        System.out.printf("[Constructor %s %s] insert code before successfully%n", this.ctClass.getName(), this.constructor.getName());
        return this;
    }

    public BuilderConstructor appendAfter(boolean needFinally, boolean needRedundant, String script) throws CannotCompileException {
        this.constructor.insertAfter(script, needFinally, needRedundant);
        System.out.printf("[Constructor %s %s] insert code after successfully%n", this.ctClass.getName(), this.constructor.getName());
        return this;
    }

    public BuilderConstructor appendAt(int atIndex, boolean needModify, String script) throws CannotCompileException {
        this.constructor.insertAt(atIndex, needModify, script);
        System.out.printf("[Constructor %s %s] insert code at %s successfully%n", this.ctClass.getName(), this.constructor.getName(), atIndex);
        return this;
    }

    @Override
    public BuilderConstructor appendCatch(String exception, String name, String script) throws NotFoundException, CannotCompileException {
        ClassPool pool = ctClass.getClassPool();
        if (Objects.isNull(exception) || JavassistTag.EMPTY.equals(exception.trim()))
            exception = JavassistTag.DEFAULT_CATCH_PATH;
        if (Objects.isNull(name) || JavassistTag.EMPTY.equals(name.trim()))
            name = JavassistTag.DEFAULT_CATCH_EXCEPTION_NAME;
        this.constructor.addCatch(script, pool.get(exception), name);
        System.out.printf("[Constructor %s %s] insert code catch successfully%n", this.ctClass.getName(), this.constructor.getName());
        return this;
    }

}