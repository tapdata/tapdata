package com.tapdata.generator;

import com.tapdata.bean.Model;
import com.tapdata.config.Config;
import com.tapdata.utils.MapUtils;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description: jsonSchema为object的时候解析
 */
public class ObjectGenerator implements Generator {
    @Override
    public String getType() {
        return "object";
    }

    @Override
    public String getJavaType() {
        return "Object";
    }

    @Override
    public String write(Model model, Config config, LinkedHashMap<String, Object> hashMap, String name) {

        boolean outer = config.checkOuter();
        String className = name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
        String description = MapUtils.getAsString(hashMap, "description");
        if (model != null) {
            StringBuilder fieldBuilder = model.getFieldBuilder();
            if (description != null) {
                fieldBuilder.append("    /** ").append(description).append(" */").append("\n");
            }
            fieldBuilder.append("    private ").append(className).append(" ").append(name).append(";\n");

            StringBuilder importClassBuilder = model.getImportClassBuilder();
            importClassBuilder.append("import " + config.getPackageName() + "." + "bean" + "." + className + ";\n");
        }

        Model newModel = new Model();
        newModel.setName(className);
        newModel.setPath(config.getPackageName());
        LinkedHashMap<String, Object> properties = (LinkedHashMap<String, Object>) hashMap.get("properties");

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            LinkedHashMap<String, Object> value = (LinkedHashMap<String, Object>) entry.getValue();
            String type = (String) value.get("type");
            Generator generator = GeneratorFactory.getGenerator(type);
            generator.write(newModel, config, value, entry.getKey());
        }

        try {
            File baseFile = new File(config.getPath());
            if (!baseFile.exists()) {
                baseFile.mkdir();
            }

            if (outer) {
                File dtoFile = new File(config.getPath() + File.separator + "dto");
                if (!dtoFile.exists()) {
                    dtoFile.mkdir();
                }

                String dtoFileName = config.getPath() + File.separator + "dto" + File.separator + className + "Dto" + "." + "java";
                createBean(config, outer, dtoFileName, description, newModel);

                File entityFile = new File(config.getPath() + File.separator + "entity");
                if (!entityFile.exists()) {
                    entityFile.mkdir();
                }
                String entityFileName = config.getPath() + File.separator + "entity" + File.separator + className + "Entity" + "." + "java";
                createBean(config, outer, entityFileName, description, newModel);

                createService(config, newModel.getName(), description);
            } else {
                File beanFile = new File(config.getPath() + File.separator + "bean");
                if (!beanFile.exists()) {
                    beanFile.mkdir();
                }

                String entityFileName = config.getPath() + File.separator + "bean" + File.separator + className + "." + "java";
                createBean(config, outer, entityFileName, description, newModel);
            }
        } catch ( Exception e) {
            e.printStackTrace();
        }
        return className;

    }


    private void createBean(Config config, boolean outer, String fileName, String description, Model newModel) {
        try {
            //File file = new File(config.getPath() + File.separator + className + "." + "java");
            File file = new File(fileName);
            if (file.exists()) {
                return;
            }
            file.createNewFile();
            String dto = config.getModel();
            dto = dto.replace("${fields}", newModel.getFieldBuilder().toString());
            StringBuilder importClassBuilder = new StringBuilder(newModel.getImportClassBuilder());
            if (!outer) {
                dto = dto.replace("${path}", newModel.getPath() + "." + "bean");
                dto = dto.replace("extends BaseDto ", "");
                dto = dto.replace("@EqualsAndHashCode(callSuper = true)\r\n", "");
                dto = dto.replace("@EqualsAndHashCode(callSuper = true)\n", "");
                dto = dto.replace("import lombok.EqualsAndHashCode;\r\n", "");
                dto = dto.replace("import lombok.EqualsAndHashCode;\n", "");
                dto = dto.replace("${name}", newModel.getName());
            } else {
                if (file.getName().endsWith("Entity.java")) {
                    dto = dto.replace("@Data", "@Data\n@Document(\"" + newModel.getName().substring(0, 1).toLowerCase(Locale.ROOT) + newModel.getName().substring(1) + "\")");
                    dto = dto.replace("extends BaseDto ", "extends BaseEntity ");
                    dto = dto.replace("${path}", newModel.getPath() + "." + "entity");
                    dto = dto.replace("${name}", newModel.getName() + "Entity");
                    importClassBuilder.append("import ").append(config.getBasePackageName()).append(".").append("entity").append(".").append("BaseEntity").append(";\n");
                    importClassBuilder.append("import ").append("org.springframework.data.mongodb.core.mapping.Document;\n");
                } else {
                    dto = dto.replace("${path}", newModel.getPath() + "." + "dto");
                    dto = dto.replace("${name}", newModel.getName() + "Dto");
                    importClassBuilder.append("import ").append(config.getBasePackageName()).append(".").append("dto").append(".").append("BaseDto").append(";\n");
                }
            }
            dto = dto.replace("${importClass}", importClassBuilder.toString());
            dto = dto.replace("${Description}", description == null ? "" : description);
            writeToFile(file, dto);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToFile(File file, String data) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
        bufferedWriter.write(data);
        bufferedWriter.flush();
        bufferedWriter.close();
    }


    private void createService(Config config, String name, String description) {
        try {
            String path = config.getPath();
            String packageName = config.getPackageName();
            File controllerFile = new File(path + File.separator + "controller");
            if (!controllerFile.exists()) {
                controllerFile.mkdir();
            }

            File controllerJavaFile = new File(path + File.separator + "controller" + File.separator + name + "Controller" + ".java");

            String controllerModel = config.getControllerModel();
            controllerModel = controllerModel.replace("${pageName}", packageName);
            controllerModel = controllerModel.replace("${ModelName}", name);
            controllerModel = controllerModel.replace("${modelName}", name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1));
            controllerModel = controllerModel.replace("${date}", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd")));
            controllerModel = controllerModel.replace("${Description}", description);

            writeToFile(controllerJavaFile, controllerModel);



            File serviceFile = new File(path + File.separator + "service");
            if (!serviceFile.exists()) {
                serviceFile.mkdir();
            }

            File serviceJavaFile = new File(path + File.separator + "service" + File.separator + name + "Service" + ".java");

            String serviceModel = config.getServiceModel();
            serviceModel = serviceModel.replace("${pageName}", packageName);
            serviceModel = serviceModel.replace("${ModelName}", name);
            serviceModel = serviceModel.replace("${modelName}", name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1));
            serviceModel = serviceModel.replace("${modelName}", name);
            serviceModel = serviceModel.replace("${date}", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd")));

            writeToFile(serviceJavaFile, serviceModel);

            File repositoryFile = new File(path + File.separator + "repository");
            if (!repositoryFile.exists()) {
                repositoryFile.mkdir();
            }

            File repositoryJavaFile = new File(path + File.separator + "repository" + File.separator + name + "Repository" + ".java");

            String repositoryModel = config.getRepositoryModel();
            repositoryModel = repositoryModel.replace("${pageName}", packageName);
            repositoryModel = repositoryModel.replace("${ModelName}", name);
            repositoryModel = repositoryModel.replace("${modelName}", name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1));
            repositoryModel = repositoryModel.replace("${date}", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd")));

            writeToFile(repositoryJavaFile, repositoryModel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
