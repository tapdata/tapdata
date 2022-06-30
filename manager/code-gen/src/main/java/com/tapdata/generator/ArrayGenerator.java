package com.tapdata.generator;

import com.tapdata.bean.Model;
import com.tapdata.config.Config;
import com.tapdata.utils.IncrementUtils;
import com.tapdata.utils.MapUtils;

import java.util.LinkedHashMap;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class ArrayGenerator implements Generator {
    @Override
    public String getType() {
        return "array";
    }

    @Override
    public String getJavaType() {
        return "List";
    }

    @Override
    public String write(Model model, Config config, LinkedHashMap<String, Object> hashMap, String name) {



        StringBuilder classNameBuilder = new StringBuilder("List<");
        LinkedHashMap<String, Object> itemMap = (LinkedHashMap<String, Object>) hashMap.get("items");
        String type = (String) itemMap.get("type");
        Generator generator = GeneratorFactory.getGenerator(type);
        if (generator instanceof ObjectGenerator) {
            String className = config.getBaseClassName() + IncrementUtils.get();
            generator.write(null, config, itemMap, className);
            classNameBuilder.append(className);
        } else if (generator instanceof ArrayGenerator) {
            String returnName = generator.write(null, config, itemMap, null);
            classNameBuilder.append(returnName);
        } else {
            classNameBuilder.append(generator.getJavaType());
        }

        classNameBuilder.append(">");
        String className = classNameBuilder.toString();
        if (name != null) {
            if (model != null) {
                String imports = model.getImportClassBuilder().toString();
                if (!imports.contains("List")) {
                    model.getImportClassBuilder().append("import java.util.List;\n");
                }
                String importName = className.replace("List", "");
                importName = importName.replace("<", "");
                importName = importName.replace(">", "");
                model.getImportClassBuilder().append("import ").append(config.getPackageName()).append(".").append("bean").append(".").append(importName).append(";\n");
                StringBuilder fieldBuilder = model.getFieldBuilder();
                String description = MapUtils.getAsString(hashMap, "description");
                if (description != null) {
                    fieldBuilder.append("    /** ").append(description).append(" */").append("\n");
                }
                fieldBuilder.append("    private ").append(className).append(" ").append(name).append(";\n");
            }
        }

        return className;
    }



}
