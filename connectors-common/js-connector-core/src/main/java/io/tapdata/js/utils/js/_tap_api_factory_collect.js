
function isParam(param){
    return param != 'undefined';
}

class TapApi {
    invoker;
    constructor (invoker) {
        this.invoker = invoker;
    }
    invoke (uriOrNameStr,paramsMap,methodStr,hasInvoker){
        let result;
        if (isParam(uriOrNameStr)){
            log.error("No API name or URL was specified, unable to execute http request. ");
            return null;
        }
        if (isParam(paramsMap) && isParam(methodStr) && isParam(hasInvoker)){
            result = this.invoker.invoke(uriOrNameStr,paramsMap,methodStr,hasInvoker);
        }else if(isParam(paramsMap) && isParam(methodStr)){
            result = this.invoker.invoke(uriOrNameStr,paramsMap,methodStr);
        }else if(isParam(paramsMap)){
            result = this.invoker.invoke(uriOrNameStr,paramsMap);
        }else {
            result = this.invoker.invoke(uriOrNameStr);
        }
        if (isParam(result)){
            result = core.toMap(result);
        }
        return result;
    }
    invokeAndCache(uriOrNameStr,paramsMap,methodStr,hasInvoker){
        let result = this.invoke(uriOrNameStr,paramsMap,methodStr,hasInvoker);
        if (isParam(uriOrNameStr) && null != result){
            return tapCache.save(uriOrNameStr,result);
        }
        return  result;
    }
    getFromCache(name){
        if (!isParam(name)){
            log.info("Invalid name will not get data from cache. ");
        }
        let cacheResult = tapCache.get(name);
        return core.toMap(cacheResult);
    }
    saveToCache(key,data,saveSec){
        if (!isParam(name)){
            log.info("Invalid name will not save data to cache. ");
        }
        if (!isParam(name)){
            log.info("Invalid data will not save data to cache. ");
        }
        if (isParam(saveSec) && !isNaN(saveSec)){
            return tapCache.save(uriOrNameStr,result,saveSec);
        }else {
            return tapCache.save(uriOrNameStr,result);
        }
    }
}

function loadAPI(){
    return new TapApi(tapAPI.loadAPI());
}
function loadAPI(apiContent,type,params){
    return new TapApi(tapAPI.loadAPI(apiContent,type,params));
}
function loadAPI(params){
    return new TapApi(tapAPI.loadAPI(params));
}