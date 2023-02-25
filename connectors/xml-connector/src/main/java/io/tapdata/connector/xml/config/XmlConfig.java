package io.tapdata.connector.xml.config;

import io.tapdata.common.FileConfig;

public class XmlConfig extends FileConfig {

    private String XPath;

    public XmlConfig() {
        setFileType("xml");
    }

    public String getXPath() {
        return XPath;
    }

    public void setXPath(String XPath) {
        this.XPath = XPath;
    }
}
