package io.tapdata.supervisor.convert.wave;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JavassistHandle {
    final Builder<?> builder;

    private JavassistHandle(Builder<?> builder) {
        this.builder = builder;
    }

    public static JavassistHandle handle(Builder<?> builder) {
        if (Objects.isNull(builder)) return null;
        return new JavassistHandle(builder);
    }

    public static List<JavassistHandle> handles(List<?> builders) {
        List<JavassistHandle> list = new ArrayList<>();
        if (Objects.nonNull(builders) && !builders.isEmpty()) {
            builders.stream().filter(Objects::nonNull).forEach(bul -> {
                if (bul instanceof Builder) {
                    list.add(new JavassistHandle((Builder<?>) bul));
                }
            });
        }
        return list;
    }

    public JavassistHandle appendBefore(String script) throws CannotCompileException {
        try {
            builder.appendBefore(script);
        } catch (Exception e) {
            System.out.println("[WARN CODE] Append code before error, will be ignore, error message is: " + e.getMessage());
        }
        return this;
    }

    public JavassistHandle appendAfter(boolean needFinally, boolean needRedundant, String script) throws CannotCompileException {
        try {
            builder.appendAfter(needFinally, needRedundant, script);
        } catch (Exception e) {
            System.out.println("[WARN CODE] Append code after error, will be ignore, error message is: " + e.getMessage());
        }
        return this;
    }

    public JavassistHandle appendAt(int atIndex, boolean needModify, String script) throws CannotCompileException {
        try {
            builder.appendAt(atIndex, needModify, script);
        } catch (Exception e) {
            System.out.println("[WARN CODE] Append code normal error, will be ignore, error message is: " + e.getMessage());
        }
        return this;
    }

    public JavassistHandle appendCatch(String exception, String name, String script) throws NotFoundException, CannotCompileException {
        try {
            builder.appendCatch(exception, name, script);
        } catch (Exception e) {
            System.out.println("[WARN CODE] Append catch code error, will be ignore, error message is: " + e.getMessage());
        }
        return this;
    }
}
