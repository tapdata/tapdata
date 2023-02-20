package io.tapdata.common;

import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;
import io.tapdata.common.util.APIUtil;
import io.tapdata.common.util.ScriptUtil;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;

import java.util.Map;
import java.util.Objects;

import static io.tapdata.base.ConnectorBase.fromJson;

/**
 * @author aplomb
 */
@Implementation(APIFactory.class)
public class APIFactoryImpl implements APIFactory {

    @Override
    public APIInvoker loadAPI(String apiJson, Map<String, Object> params) {
        Map<String, Object> json = null;
        if (Objects.nonNull(apiJson)) {
            try {
                json = (Map<String, Object>) fromJson(apiJson);
            } catch (Exception e) {
                try {
                    json = (Map<String, Object>) fromJson(ScriptUtil.loadFileFromJarPath(apiJson));
                } catch (Throwable ignored) {
                    //TapLogger.error(TAG, "Can't analysis api JSON document from path: " + apiJson + ". ");
                    throw new CoreException("The current type is temporarily not supported. ");
                }
            }
        } else {
            try {
                json = (Map<String, Object>) fromJson(ScriptUtil.loadFileFromJarPath(APIFactory.DEFAULT_POST_MAN_FILE_PATH));
            } catch (Throwable ignored) {
                //TapLogger.error(TAG, "Can't analysis api JSON document, please save this JSON file into " + this.analysis.sourcePath() + ". ");
                throw new CoreException("The current type is temporarily not supported. ");
            }
        }
        String type = APIUtil.ApiType(json);
        switch (type) {
            case APIUtil.TYPE_POSTMAN:
                return PostManAPIInvoker.create().analysis(json, params);
            //case APIFactory.TYPE_API_FOX: return PostManAPIInvoker.create().analysis(apiContent,params);
        }
        throw new CoreException(String.format("The current type is temporarily not supported. Not supported: %s .", type));
    }

    @Override
    public APIInvoker loadAPI(Map<String, Object> params) {
        return loadAPI(null, params);
    }

    @Override
    public APIInvoker loadAPI() {
        return loadAPI(null, null);
    }

}
