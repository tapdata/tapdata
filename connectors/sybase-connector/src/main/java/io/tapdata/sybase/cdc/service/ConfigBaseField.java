package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.sybase.SybaseConnector;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.util.Utils;
import io.tapdata.sybase.util.ZipUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.UUID;

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
//        String cdcId = null;
//        if (null != root.getContext() && null != root.getContext().getId()) {
//            cdcId = root.getContext().getId();
//        }
//        if (null == cdcId) {
//            cdcId = UUID.randomUUID().toString().replaceAll("-", "_");
//        }
//        root.setCdcId(cdcId);
//        String targetPath = "sybase-poc-temp/";// + cdcId + "/";
//        File sybasePocPath = new File(targetPath);
//        if (!sybasePocPath.exists() || !sybasePocPath.isDirectory()) sybasePocPath.mkdir();
//        String pocPathFromLocal = getPocPathFromLocal();
//        File fromLocal = new File(pocPathFromLocal);
//        if (fromLocal.exists() && fromLocal.isDirectory()){
//            try {
//                if ("linux".equalsIgnoreCase(System.getProperty("os.name"))) {
//                    Utils.run("cp -r " + (pocPathFromLocal.endsWith("/") ? (pocPathFromLocal + "*") : (pocPathFromLocal + "/*")) + " " + targetPath);
//                } else {
//                    FileUtils.copyToDirectory(fromLocal, sybasePocPath);
//                }
//            } catch (Exception e){
//                throw new CoreException("Unable init cdc tool from path {}, make sure your sources exists in you linux file system or retry task.", pocPathFromLocal);
//            }
//        } else {
//            ZipUtils.unzip(pocPathFromLocal, sybasePocPath);
//        }
//        String absolutePath = sybasePocPath.getAbsolutePath();
//        try {
//            absolutePath = URLDecoder.decode(absolutePath, "utf-8");
//        } catch (Exception ignore) {
//        }
        String targetPath = "sybase-poc";
        File sybasePocPath = new File(targetPath);
        if (!sybasePocPath.exists() || !sybasePocPath.isDirectory()){
            throw new CoreException("Unable fund {}, make sure your sources exists in you linux file system.", sybasePocPath.getAbsolutePath());
        }
        this.root.setSybasePocPath(sybasePocPath.getAbsolutePath());
        return this.root;
    }

    private String getPocPathFromResources() {
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

    private String getPocPathFromLocal() {
        boolean isLinuxCore = "linux".equalsIgnoreCase(System.getProperty("os.name"));
        String pocPath = isLinuxCore ? "sybase-poc.zip" : "D:\\sybase-poc.zip";
        File file = new File(pocPath);
        if (!file.exists() || !file.isFile()) {
            if (isLinuxCore) {
                file = new File("sybase-poc");
                if (file.exists() && file.isDirectory()) {
                    return file.getAbsolutePath();
                }
                try {
                    return getPocPathFromResources();
                } catch (Exception e) {
                    throw new CoreException("Unable fund {}, make sure your sources exists in you linux file system.", file.getAbsolutePath());
                }
            } else {
                throw new CoreException("Unable fund {}, make sure your sources exists in you windows file system.", file.getAbsolutePath());
            }
        }
        return pocPath;
    }
}
