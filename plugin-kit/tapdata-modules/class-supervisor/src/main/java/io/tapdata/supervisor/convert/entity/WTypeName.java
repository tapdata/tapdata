package io.tapdata.supervisor.convert.entity;

import io.tapdata.supervisor.utils.ClassUtil;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class WTypeName extends WBaseTarget {
    @Override
    public WTypeName parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        return this;
    }

    @Override
    public String[] freshenPaths() {
        String packagePath = this.scanPackage.replaceAll("((\\.?)(\\*{1,2}))", "").replaceAll("^[\\.]*(.*)", "$1");
        Set<Class<?>> classSet = ClassUtil.getClass(Object.class, packagePath);
        classSet = classSet.stream().filter(cla -> {
            String simpleName = cla.getSimpleName();
            return simpleName.matches(this.path);
        }).collect(Collectors.toSet());
        String[] arr = new String[classSet.isEmpty() ? 0 : classSet.size()];
        int index = 0;
        for (Class<?> aClass : classSet) {
            arr[index++] = aClass.getName();
        }
        return arr;
    }
}
