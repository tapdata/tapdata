package io.tapdata.supervisor.convert.wave;

import javassist.CannotCompileException;
import javassist.NotFoundException;

interface Builder<T> {
    public T appendBefore(String script) throws CannotCompileException ;

    public T appendAfter(boolean needFinally, boolean needRedundant, String script) throws CannotCompileException;

    public T appendAt(int atIndex, boolean needModify, String script) throws CannotCompileException ;

    public T appendCatch(String exception,String name,String script) throws NotFoundException, CannotCompileException;
}
