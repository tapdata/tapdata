package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.JavassistTag;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import java.util.List;
import java.util.Objects;

public class CtClassGetter {
    public static CtClass byName(ClassPool pool, String name) throws NotFoundException {
       return pool.get(name);
    }
    public static CtClass[] byName(ClassPool pool, List<String> names) throws NotFoundException{
        CtClass[] cls = new CtClass[Objects.isNull(names) || names.isEmpty() ? JavassistTag.ZERO : names.size()];
        if (Objects.nonNull(names) && !names.isEmpty()) {
            for (int index = JavassistTag.ZERO ; index < names.size() ; index++) {
                cls[index] = pool.get(names.get(index));
            }
        }
        return cls;
    }
}
