/**
 * @type public class
 * @author Gavin
 * @description API操作/调用类
 * */
class TapApi {
    /**
     * @type prototype
     * @description API 执行器，
     * */
    invoker;
    /**
     * @type prototype
     * @description API执行的全局参数（connectionConfig + nodeConfig）
     * */
    config;
    /**
     * @type prototype
     * @description API执行的HTTP配置：包含超时时间设置；
     * */
    httpConfigParam = null;
    httpConfigGlobal = {
        'timeout': 3000 //HTTP调用的超时等待时间,默认3000ms
    };

    /**
     * 构造方法
     * @param invoker API 执行器，通过tapAPI.loadAPI()获取
     * */
    constructor(invoker) {
        this.invoker = invoker;
        this.config = _tapConfig_;
    }

    /**
     * @description 设置HTTP调用的超时等待时间
     * @param milliseconds HTTP调用的超时等待时间,单位：ms
     * */
    setTimeOut(milliseconds) {
        if (Number.isNaN(milliseconds)) {
            return;
        }
        this.httpConfigParam.timeout = milliseconds;
        return this;
    }

    /**
     * @description 设置API执行的HTTP配置
     * @param configMap API执行的HTTP配置，Object
     * @deprecated
     * */
    httpConfigAsGlobal(configMap) {
        if ('undefined' === configMap || null == configMap) return this;
        this.httpConfigGlobal = configMap;
        return this;
    }

    httpConfig(configMap) {
        if ('undefined' === configMap || null == configMap) return this;
        this.httpConfigParam = configMap;
        return this;
    }

    /**
     * @description 执行API,获取Http调用结果，此接口在执行后会回调update_token()方法，
     *               -需要实现update_token(),并依据update_token()的内容来执行相关的处理动作，例如更新token
     * @param uriOrNameStr 需要调用的API名称或者URL，建议使用唯一性的名称。与之对应的部分是 postman_api_collection.json 中API接口的 name 字段值。
     * @param paramsMap 需要调用的API时传递的接口参数，使用这些参数进行Http调用，接口使用参数的优先级在此处声明的变量具有最高使用优先级，
     *                  - 具体的优先级为：paramsMap中的变量 > 使用tapAPI.loadAPI(...)声明的接口变量 > postman_api_collection.json对接口配置的变量
     * @param methodStr 需要调用的API时执行的Http类型，为POST|GET|PUT|PATH|DELETE...,建议直接在postman_api_collection.json中对API配置，此时可以忽略此参数。
     *
     * @return Object API通过Http请求后返回的结果：
     *     - {
     *         "result"：Object, // 结果的结构视具体API而定
     *         "httpCode": “”，  // Http请求状态码：200 <= httpCode < 300时为正常的http结果，其他表示http调用失败
     *         “headers”: {},    // Http请求的请求头
     *         "error": {
     *             "msg": ""
     *         }    // Http请求的错误信息
     *     }
     *  */
    invoke(uriOrNameStr, paramsMap, methodStr) {
        if (!isParam(uriOrNameStr)) {
            log.error("No API name or URL was specified, unable to execute http request. ");
            return null;
        }
        return this.invokeMemberPrivate(
            uriOrNameStr,
            isParam(paramsMap) ? paramsMap : {},
            isParam(methodStr) ? methodStr : "POST",
            true
        );
    }

    /**
     * @description 执行API,获取Http调用结果，此接口在执行后不会回调update_token()方法，
     * @param uriOrNameStr 需要调用的API名称或者URL，建议使用唯一性的名称。与之对应的部分是 postman_api_collection.json 中API接口的 name 字段值。
     * @param paramsMap 需要调用的API时传递的接口参数，使用这些参数进行Http调用，接口使用参数的优先级在此处声明的变量具有最高使用优先级，
     *                  - 具体的优先级为：paramsMap中的变量 > 使用tapAPI.loadAPI(...)声明的接口变量 > postman_api_collection.json对接口配置的变量
     * @param methodStr 需要调用的API时执行的Http类型，为POST|GET|PUT|PATH|DELETE...,建议直接在postman_api_collection.json中对API配置，此时可以忽略此参数。
     *
     * @return Object API通过Http请求后返回的结果：
     *     - {
     *         "result"：Object, // 结果的结构视具体API而定
     *         "httpCode": “”，  // Http请求状态码：200 <= httpCode < 300时为正常的http结果，其他表示http调用失败
     *         “headers”: {},    // Http请求的请求头
     *         "error":Object    // Http请求的错误信息
     *     }
     *  */
    invokeWithoutIntercept(uriOrNameStr, paramsMap, methodStr) {
        if (!isParam(uriOrNameStr)) {
            log.error("No API name or URL was specified, unable to execute http request. ");
            return null;
        }
        return this.invokeMemberPrivate(
            uriOrNameStr,
            isParam(paramsMap) ? paramsMap : {},
            isParam(methodStr) ? methodStr : "POST",
            false
        );
    }

    /**
     * 禁止外部使用
     * @deprecated
     * */
    invokeMemberPrivate(uriOrNameStr, paramsMap, methodStr, needIntercept) {
        this.config = _tapConfig_;
        this.invoker.setConfig(null != this.httpConfigParam ? this.httpConfigParam : this.httpConfigGlobal);
        this.invoker.setConnectorConfig(this.config);
        let result = this.invoker.invoke(uriOrNameStr, paramsMap, methodStr, needIntercept);
        this.httpConfigParam = null;
        let hasResult = isParam(result);
        let resultData = tapUtil.toMap(result.result).data;
        if (resultData.toJSONString) {
            resultData = JSON.parse(resultData.toJSONString());
        }
        return {
            "result": !hasResult ? {} : resultData,
            "httpCode": !hasResult ? -1 : result.httpCode,
            "headers": !hasResult ? {} : tapUtil.toMap(result.headers)
        };
    }

    /**
     * 禁止外部使用
     * */
    addConfig(connection, node) {
        this.config = tapUtil.mixedData(connection, node);
    }

    /**
     * 未启用的方法
     * */
    invokeAndCache(uriOrNameStr, paramsMap, methodStr, hasInvoker) {
        let result = this.invoke(uriOrNameStr, paramsMap, methodStr, hasInvoker);
        if (isParam(uriOrNameStr) && null != result) {
            //return tapCache.save(uriOrNameStr, result);
        }
        return result;
    }

    /**
     * 未启用的方法
     * */
    getFromCache(name) {
        if (!isParam(name)) {
            log.info("Invalid name will not get data from cache. ");
        }
        let cacheResult = null;//tapCache.get(name);
        return core.toMap(cacheResult);
    }

    /**
     * 未启用的方法
     * */
    saveToCache(key, data, saveSec) {
        if (!isParam(name)) {
            log.info("Invalid name will not save data to cache. ");
        }
        if (!isParam(name)) {
            log.info("Invalid data will not save data to cache. ");
        }
        if (isParam(saveSec) && !isNaN(saveSec)) {
            //return tapCache.save(uriOrNameStr, result, saveSec);
        } else {
            //return tapCache.save(uriOrNameStr, result);
        }
    }

    /**
     * 未启用的方法
     * */
    releaseFromCache(key) {
        if (!isParam(key)) {
            log.info("Key name is empty, cannot release data. ");
        }
        //return tapCache.release(key);
    }
}

/**
 * @author Gavin
 * @description 加载API执行器
 * @param params 定义API执行是的Http参数，声明的变量会作用与全局，但受优先级限制
 *          - 具体的优先级为：使用TapApi.invoker(paramsMap)时的的变量 > 使用tapAPI.loadAPI(...)声明的接口变量 > postman_api_collection.json对接口配置的变量
 * @param apiContent postman_api_collection.json内容或postman_api_collection.json的相对路径。用于加载指定的postman_api_collection.json，不填时默认加载Source Root路径下的postman_api_collection.json：
 *                  - 默认Source Root路径为：postman_api_collection.json
 * @return TapApi API执行器,用于执行 postman_api_collection.json 中声明的API
 * @date 2023/2/13
 * */
function loadAPI(params, apiContent) {
    if (isParam(apiContent) && isParam(params)) {
        return new TapApi(tapAPI.loadAPI(apiContent, params));
    } else if (isParam(params)) {
        return new TapApi(tapAPI.loadAPI(params));
    } else {
        return new TapApi(tapAPI.loadAPI());
    }
}

/**
 * @type global variable
 * @author Gavin
 * @description 配置任务或环境下的属性变量
 * @date 2023/2/13
 * */
var config = {
    /**
     * @type function
     * @author Gavin
     * @description 设置增量轮询的时间间隔
     * @param interval 秒为单位的Number,增量时间间隔
     * @return void
     * @date 2023/2/13
     * */
    setStreamReadIntervalSeconds: function (interval) {
        tapConfig.setStreamReadIntervalSeconds(interval);
    },
    /**
     * @type function
     * @author Gavin
     * @description 获取增量轮询的时间间隔
     * @return 秒为单位的Number,增量时间间隔
     * @date 2023/2/13
     * */
    getStreamReadIntervalSeconds: function () {
        return tapConfig.getStreamReadIntervalSeconds();
    }
}

/**
 * @type function
 * @author Gavin
 * @description 判断变量是否被定义
 * @return boolean
 * @date 2023/2/13
 * */
function isParam(param) {
    return typeof (param) != 'undefined';
}

/**
 * @type function
 * @author Gavin
 * @description 判断任务是否终止
 * @return boolean
 * @date 2023/2/13
 * */
function isAlive() {
    return nodeIsAlive.get();
}

/**
 * @type function
 * @author Gavin
 * @description 按指定毫秒时间休眠
 * @param millisecond Number 单位：毫秒
 * @date 2023/2/13
 * */
function sleep(millisecond) {

}

function cloneObj(obj) {
    // let newObj = obj instanceof Array ? [] : {};
    // for(let param in obj){
    //     let item = obj[param];
    //     newObj[param] = typeof item === 'object' ? cloneObj(obj) : item;
    // }
    // return newObj;
    return JSON.parse(JSON.stringify(obj));
}

function sha256_HMAC(value, key){
    return tapUtil.sha256_HMAC(value, key);
}

function getConfig(key){
    return _tapConfig_.get(key);
}

