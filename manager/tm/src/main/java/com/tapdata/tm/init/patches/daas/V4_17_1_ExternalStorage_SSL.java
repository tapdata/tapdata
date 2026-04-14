package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.SSLFileUtil;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.nio.file.Paths;

@PatchAnnotation(appType = AppType.DAAS, version = "4.17-1")
public class V4_17_1_ExternalStorage_SSL extends AbsPatch {
    public V4_17_1_ExternalStorage_SSL(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        Environment environment = SpringContextHelper.getBean(Environment.class);
        Boolean ssl = environment.getProperty("spring.data.mongodb.ssl", Boolean.class,false);
        if(ssl){
            String caPath = environment.getProperty("spring.data.mongodb.caPath", String.class);
            String keyPath = environment.getProperty("spring.data.mongodb.keyPath", String.class);
            String sslPass = environment.getProperty("spring.data.mongodb.sslPass", String.class);
            ExternalStorageService externalStorageService = SpringContextHelper.getBean(ExternalStorageService.class);
            Update update = new Update();
            update.set("ssl", true);
            if (StringUtils.isNotBlank(keyPath)) {
                update.set("sslKey", SSLFileUtil.readAndValidatePrivateKey(keyPath));
                String fileName = Paths.get(keyPath).getFileName().toString();
                update.set("attrs.sslKeyFile",fileName);
            }
            if (StringUtils.isNotBlank(sslPass)) {
                update.set("sslPass", sslPass);
            }
            if(StringUtils.isNotBlank(caPath)){
                update.set("sslValidate", true);
                update.set("sslCA", SSLFileUtil.readAndValidateCertificate(caPath));
                String fileName = Paths.get(caPath).getFileName().toString();
                update.set("attrs.sslCAFile",fileName);
            }

            UpdateResult updateResult = externalStorageService.update(new Query(Criteria.where("name").is("Tapdata MongoDB External Storage")), update);
        }
    }
}
