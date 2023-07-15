package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.sybase.SybaseConnector;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.util.ZipUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

/**
 * @author GavinXiao
 * @description ConfigBaseField create by Gavin
 * @create 2023/7/13 11:08
 **/
public class ConfigBaseField implements CdcStep<CdcRoot> {
    CdcRoot root;
    String path;

    public ConfigBaseField(CdcRoot root, String path) {
        this.root = root;
        this.path = path;
//        File cdcFile = new File(path);
//        if (cdcFile.isDirectory()) {
//            root.setCdcFile(cdcFile);
//        } else {
//            throw new CoreException("Can not find sybase-poc directory, path: " + path);
//        }
    }

    @Override
    public CdcRoot compile() {
        File sybasePocPath = new File("sybase-poc/");
        if (!sybasePocPath.exists() || !sybasePocPath.isDirectory()) sybasePocPath.mkdir();
        ZipUtils.unzip(getPocPathFromLocal(11), sybasePocPath);
        String absolutePath = sybasePocPath.getAbsolutePath();
        try {
            absolutePath = URLDecoder.decode(absolutePath, "utf-8");
        } catch (Exception ignore) {
        }
        this.root.setSybasePocPath(FilenameUtils.concat(absolutePath , "/sybase-poc/"));
        return this.root;
    }

    private String getPocPathFromLocal(){
        URL resource = null;
        try {
            resource = SybaseConnector.class.getClassLoader().getResource("sybase-poc.zip");
        } catch (Exception e) {
            try {
                Enumeration<URL> resources = SybaseConnector.class.getClassLoader().getResources("sybase-poc.zip");
                while (resources.hasMoreElements()) {
                    URL url = resources.nextElement();
                    if (url.getFile().contains("sybase-poc.jar")) {
                        resource = url;
                        break;
                    }
                }
            } catch (Exception ex) {
                throw new CoreException("Unable get sybase-poc from /resource, please make sure sybase-poc is exist.");
            }
        }
        if (null == resource) {
            throw new CoreException("Unable get sybase-poc from /resource, please make sure sybase-poc is exist.");
        }
        return resource.getPath();
    }

    private String getPocPathFromLocal(int type){
        if (type > 0) {
            return getPocPathFromLocal();
        } else {
            return "/sybase-poc/sybase-poc.tar.gz";
        }
    }
}
