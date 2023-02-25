package io.tapdata.bigquery.service.stage.tapvalue;

import io.tapdata.entity.schema.type.TapType;

import java.util.HashMap;
import java.util.Map;

public class ValueHandel {
    Map<Class<? extends TapType>,TapValueForBigQuery> handel = new HashMap<>();

    public ValueHandel(){}

    public static ValueHandel create() {
        return new ValueHandel();
    }

    private Map<Class<? extends TapType>,TapValueForBigQuery> handel(){
        return this.handel;
    }
    private TapValueForBigQuery handel(Class<? extends TapType> fieldClz){
        if (null == fieldClz) return null;
        if (handel.containsKey(fieldClz)) {
            return handel.get(fieldClz);
        }
        try {
            String name = fieldClz.getSimpleName();
            Class<?> aClass = Class.forName(TapValueForBigQuery.class.getPackage().getName() + "." + name + "Sql");
            TapValueForBigQuery tapValueStage = (TapValueForBigQuery)aClass.newInstance();
            this.handel.put(fieldClz,tapValueStage);
            return tapValueStage;
        } catch (ClassNotFoundException e) {

        } catch (IllegalAccessException e) {

        } catch (InstantiationException e) {

        }
        return null;
    }

    public String sqlValue(Object value, Class<? extends TapType> fieldClz){
        if (null == value) return "NULL";
        TapValueForBigQuery handel = this.handel(fieldClz);
        if (null != handel) return handel.value(value);
        return TapValueForBigQuery.simpleStringValue(value);
    }

}
