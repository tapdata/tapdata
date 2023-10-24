package io.tapdata.pdk.apis.spec;

import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.utils.DataMap;

import java.util.List;
import java.util.Map;

/**
 * Node specification to register node with form components.
 * When any one is creating a node, need to input information base on form components.
 * <p>
 * Node could be a Source, Target or Processor.
 */
public class TapNodeSpecification {
    private String name;
    private String id;
    private String group; //Unique key for each enterprise.
    private String version;
    private String icon;
    private DataMap configOptions; //include connection and node
    private DefaultExpressionMatchingMap dataTypesMap;

    private Map<String, String> manifest;
    private String authentication;
    private List<String> tags;

    public String verify() {
        if(name == null)
            return "missing name";
        if(id == null)
            return "missing id";
        if(group == null)
            return "missing group";
        if(version == null)
            return "missing version";
        return null;
    }

    public String toString() {
        return "TapNodeSpecification name: " + name + " id: " + id + " group: " + group + " version: " + version;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String idAndGroup() {
        return id + "@" + group + "-v" + version;
    }

    public static String idAndGroup(String id, String group, String version) {
        return id + "@" + group + "-v" + version;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public DataMap getConfigOptions() {
        return configOptions;
    }

    public void setConfigOptions(DataMap configOptions) {
        this.configOptions = configOptions;
    }

    public DefaultExpressionMatchingMap getDataTypesMap() {
        return dataTypesMap;
    }

    public void setDataTypesMap(DefaultExpressionMatchingMap dataTypesMap) {
        this.dataTypesMap = dataTypesMap;
    }

    public Map<String, String> getManifest() {
        return manifest;
    }

    public void setManifest(Map<String, String> manifest) {
        this.manifest = manifest;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
