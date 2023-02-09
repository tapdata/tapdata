package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Lists;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.cli.CommonCli;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.cli.MavenCli;
import org.codehaus.plexus.classworlds.ClassWorld;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.util.List;

/**
 * @author Dexter
 */
@CommandLine.Command(
        description = "Init a connector project",
        subcommands = MainCli.class
)
public class ConnectorProjectBootCli extends CommonCli {
    private static final Logger logger = LoggerFactory.getLogger(ConnectorProjectBootCli.class.getSimpleName());
    private static final String TAG = ConnectorProjectBootCli.class.getSimpleName();

//    @CommandLine.Option(names = {"-t", "--type"}, required = false, description = "The type of connector, source or target")
//    private String type;

    @CommandLine.Option(names = {"-g", "--group"}, required = false, description = "The group id of the connector")
    private String groupId;

    @CommandLine.Option(names = {"-n", "--name"}, required = true, description = "The name of the connector")
    private String artifactId;

    @CommandLine.Option(names = {"-v", "--version"}, required = true, description = "The version of the connector")
    private String version;

    @CommandLine.Option(names = {"-o", "--output"}, required = true, description = "The location of the connector project")
    private String output;

    @Override
    protected Integer execute() throws Exception {
        List<String> paramsList = Lists.newArrayList();

//        switch (type) {
//            case "source":
//                paramsList.add("-DarchetypeArtifactId=source-connector-archetype");
//                break;
//            case "target":
//                paramsList.add("-DarchetypeArtifactId=target-connector-archetype");
//                break;
//            case "targetNeedTable":
//                paramsList.add("-DarchetypeArtifactId=target-need-table-connector-archetype");
//                break;
//            default:
//                throw new IllegalArgumentException("Type is illegal, expect source, target or targetNeedTable, but " + type);
//        }
        paramsList.add("-DarchetypeArtifactId=connector-archetype");
        paramsList.add("archetype:generate");
        paramsList.add("-DarchetypeGroupId=io.tapdata");
        paramsList.add("-DarchetypeVersion=1.0.0");
        paramsList.add("-DinteractiveMode=false");
        //debug only
        paramsList.add("-DarchetypeCatalog=local");
        paramsList.add(String.format("-DgroupId=%s", groupId));
        paramsList.add(String.format("-DartifactId=%s-connector", artifactId.toLowerCase()));
        paramsList.add(String.format("-Dpackage=%s.%s", groupId, artifactId.toLowerCase()));
        paramsList.add(String.format("-DlibName=%s", artifactId));

		output = FilenameUtils.concat(System.getProperty("user.dir"), output);
//        paramsList.add(String.format("-DoutputDirectory=%s", output));

//        MavenCli mavenCli = new MavenCli(new ClassWorld("maven", getClass().getClassLoader()));
//        System.setProperty("maven.multiModuleProjectDirectory", ".");
//        String[] params = paramsList.toArray(new String[]{});
//        return mavenCli.doMain(params, "..", System.out, System.err
//        );
        paramsList.add(String.format("-DoutputDirectory=%s", output));

        MavenCli mavenCli = new MavenCli(new ClassWorld("maven", getClass().getClassLoader()));
        System.setProperty("maven.multiModuleProjectDirectory", ".");
        String[] params = paramsList.toArray(new String[]{});
        ByteArrayOutputStream outBaos = new ByteArrayOutputStream();
        ByteArrayOutputStream errBaos = new ByteArrayOutputStream();
        PrintStream outps = new PrintStream(outBaos);
        PrintStream errps = new PrintStream(errBaos);
        int state = mavenCli.doMain(params, "../..", outps,errps);
        if (0 == state){
            setSpecNameAndId();
            System.out.println(outBaos);
            System.out.println("------------- Template generate successfully -------------");
            System.out.println("|="+artifactId+" connector project location: " + output + "/" + artifactId.toLowerCase() + "-connector");
            System.out.println("|="+artifactId+" connector spec file: " + output + "/" + artifactId.toLowerCase() + "-connector/src/main/resources/spec.json");
        }
        else {
            System.out.println(outBaos);
            System.out.println(errBaos);
            System.out.println("Please check and remove it if the folder \""+ output + "/" + artifactId.toLowerCase()  +"\" already exists");
            System.out.println("------------- Template generate failed --------------");
        }

        return state;
    }


    private void setSpecNameAndId() throws IOException {
        String specPath = output + "/" + artifactId.toLowerCase() + "-connector/src/main/resources/spec.json";
        String specJson = FileUtils.readFileToString(new File(specPath), "utf8");
        JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
        DataMap dataMap = jsonParser.fromJsonObject(specJson);
        DataMap propertyMap = jsonParser.fromJsonObject(jsonParser.toJson(dataMap.get("properties")));
        propertyMap.put("name", artifactId);
        propertyMap.put("id", artifactId.toLowerCase());
        dataMap.put("properties", propertyMap);
        String outputSpec = JSON.toJSONString(dataMap, SerializerFeature.PrettyFormat);
        FileUtils.writeStringToFile(new File(specPath), outputSpec, "utf-8");
    }
}
