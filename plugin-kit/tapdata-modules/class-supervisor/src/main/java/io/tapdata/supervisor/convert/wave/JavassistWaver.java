package io.tapdata.supervisor.convert.wave;

import io.tapdata.supervisor.utils.DependencyURLClassLoader;
import io.tapdata.supervisor.utils.JavassistTag;
import javassist.*;

import java.io.IOException;
import java.util.*;

public class JavassistWaver {
    private final ClassPool pool;
    DependencyURLClassLoader dependencyURLClassLoader;

    public static JavassistWaver create(DependencyURLClassLoader dependencyURLClassLoader) {
        return new JavassistWaver(dependencyURLClassLoader);
    }

    private JavassistWaver(DependencyURLClassLoader dependencyURLClassLoader) {
        this.pool = ClassPool.getDefault();
        this.pool.appendClassPath(new LoaderClassPath(dependencyURLClassLoader));
        this.dependencyURLClassLoader = dependencyURLClassLoader;
    }

    private JavassistWaver(ClassPool pool) {
        this.pool = Optional.ofNullable(pool).orElse(ClassPool.getDefault());
        this.pool.appendClassPath(new LoaderClassPath(dependencyURLClassLoader));
    }


    public Builder builder(String path, String saveTo) throws NotFoundException {
        try {
            return new Builder(this.pool, path, saveTo);
//            Class<?> aClass = Class.forName(path);
//            int modifiers = aClass.getModifiers();
//            if (!Modifier.isFinal(modifiers)) {
//                return new Builder(this.pool, path, saveTo);
//            }else {
//                System.out.println("[WARN] Class is final, cannot be modify. " + path + " will be ignore.");
//                return null;
//            }
        }catch (Exception ignored){
            return null;
        }
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
                String claInfo = this.ctClass.toString();
                System.out.printf("[WARN GET CONSTRUCTOR] Class info - %s, Constructor args - [%s], return type - %s. %s.%n", claInfo, this.showList(args), returnType, e.getMessage());
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
                    String claInfo = this.ctClass.toString();
                    System.out.printf("[WARN GET CONSTRUCTOR] Class info - %s, Signature - %s, %s.%n", claInfo, constructor.getSignature(), e.getMessage());
                }
            }
            return list;
        }

        public io.tapdata.supervisor.convert.wave.Builder<BuilderMethod> method(String name, List<String> args, String returnType, boolean createNotExist, String createWith) throws NotFoundException {
            try {
                return new BuilderMethod(this.ctClass, name, args, returnType, createNotExist, createWith);
            } catch (Exception e) {
                String claInfo = this.ctClass.toString();
                System.out.printf("[WARN GET METHOD] Class info - %s, method - %s, method args - [%s], return type - %s. %s.%n", claInfo, name, this.showList(args), returnType, e.getMessage());
                return null;
            }
        }

        public void build() throws IOException, CannotCompileException, NotFoundException {
            this.defrost();
            if (Objects.isNull(this.saveTo) || this.saveTo.trim().equals(JavassistTag.EMPTY)) {
                this.ctClass.writeFile();
            } else {
                this.ctClass.writeFile(this.saveTo);
            }
            this.defrost();
            ctClass.setSuperclass(ctClass.getSuperclass());
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

        private void defrost() throws NotFoundException, CannotCompileException {
            if (ctClass.isFrozen()) {
                ctClass.defrost();
                ctClass.setSuperclass(ctClass.getSuperclass());
            }
        }
    }
}
