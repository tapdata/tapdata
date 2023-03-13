package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.JavassistTag;
import javassist.*;

import java.io.IOException;
import java.util.*;

public class JavassistWaver {
    private final ClassPool pool;

    public static JavassistWaver create() {
        return new JavassistWaver();
    }

    private JavassistWaver() {
        this.pool = ClassPool.getDefault();
    }

    private JavassistWaver(ClassPool pool) {
        this.pool = Optional.ofNullable(pool).orElse(ClassPool.getDefault());
    }

    public Builder builder(String path, String saveTo) throws NotFoundException {
        return new Builder(this.pool, path, saveTo);
    }

    public static class Builder {
        CtClass ctClass;
        ClassPool pool;
        String saveTo;

        private Builder(ClassPool pool, String path, String saveTo) throws NotFoundException {
            this.pool = pool;
            this.ctClass = pool.get(path);
            this.saveTo = saveTo;
        }

        public io.tapdata.supervisor.convert.wave.Builder<BuilderConstructor> constructor(List<String> args, String returnType) throws NotFoundException {
            try {
                return new BuilderConstructor(this.ctClass, args, returnType);
            } catch (Exception e) {
                String claName = "Unknow Class";
                String nameClass = "Unknow Class Name";
                try {
                    claName = this.ctClass.toClass().getName();
                    nameClass = this.ctClass.toClass().getSimpleName();
                } catch (Exception e1) {
                    claName = claName + "(" + e1.getMessage() + ")";
                }
                System.out.printf("[WARN GET CONSTRUCTOR] Class - %s, Constructor - %s, Constructor args - [%s], return type - %s. %s.%n", claName, nameClass, this.showList(args), returnType, e.getMessage());
                return null;
            }
        }

        public List<?> constructorAll() throws NotFoundException {
            CtConstructor[] constructors = this.ctClass.getConstructors();
            List<io.tapdata.supervisor.convert.wave.Builder<BuilderConstructor>> list = new ArrayList<>();
            for (CtConstructor constructor : constructors) {
                try {
                    list.add(new BuilderConstructor(this.ctClass, constructor));
                } catch (Exception e) {
                    String claName = "Unknow Class";
                    String nameClass = "Unknow Class Name";
                    try {
                        claName = this.ctClass.toClass().getName();
                        nameClass = this.ctClass.toClass().getSimpleName();
                    } catch (Exception e1) {
                        claName = claName + "(" + e1.getMessage() + ")";
                    }
                    System.out.printf("[WARN GET CONSTRUCTOR] Class - %s, Constructor - %s, Signature - %s, %s.%n", claName, nameClass, constructor.getSignature(), e.getMessage());
                }
            }
            return list;
        }

        public io.tapdata.supervisor.convert.wave.Builder<BuilderMethod> method(String name, List<String> args, String returnType, boolean createNotExist, String createWith) throws NotFoundException {
            try {
                return new BuilderMethod(this.ctClass, name, args, returnType, createNotExist, createWith);
            } catch (Exception e) {
                String claName = "Unknow Class";
                String nameClass = "Unknow Class Name";
                try {
                    claName = this.ctClass.toClass().getName();
                    nameClass = this.ctClass.toClass().getSimpleName();
                } catch (Exception e1) {
                    claName = claName + "(" + e1.getMessage() + ")";
                }
                System.out.printf("[WARN GET METHOD] Class - %s, method - %s, method args - [%s], return type - %s. %s.%n", claName, nameClass, this.showList(args), returnType, e.getMessage());
                return null;
            }
        }

        public void build() throws IOException, CannotCompileException, NotFoundException {
            if (Objects.isNull(this.saveTo) || this.saveTo.trim().equals(JavassistTag.EMPTY)) {
                this.ctClass.writeFile();
            } else {
                this.ctClass.writeFile(this.saveTo);
            }
        }

        private String showList(List<String> args) {
            StringJoiner joiner = new StringJoiner(", ");
            if (Objects.isNull(args) || args.isEmpty()) joiner.add("void");
            else {
                for (String arg : args) {
                    joiner.add(arg);
                }
            }
            return joiner.toString();
        }
    }
}
