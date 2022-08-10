package io.tapdata.common.ddl;

import io.tapdata.common.ddl.parser.DDLParser;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.type.DDLType;
import io.tapdata.common.ddl.type.WrapperType;
import io.tapdata.common.ddl.wrapper.DDLWrapper;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.Capability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 12:17
 **/
public class DDLFactory {

    public static List<Capability> getCapabilities(DDLParserType ddlParserType) {
        if (null == ddlParserType) {
            return Collections.EMPTY_LIST;
        }
        List<String> capabilityIds = new ArrayList<>();
        List<Capability> capabilities = new ArrayList<>();
        Class<? extends WrapperType> wrapperType = ddlParserType.getWrapperType();
        WrapperType wrapperTypeBean = InstanceFactory.bean(wrapperType);
        List<DDLType> ddlTypes = wrapperTypeBean.getDdlTypes();
        for (DDLType ddlType : ddlTypes) {
            Class<? extends DDLWrapper<?>>[] ddlWrappers = ddlType.getDdlWrappers();
            for (Class<? extends DDLWrapper<?>> ddlWrapper : ddlWrappers) {
                DDLWrapper<?> ddlWrapperBean = InstanceFactory.bean(ddlWrapper);
                List<Capability> c = ddlWrapperBean.getCapabilities();
                for (Capability capability : c) {
                    if (capabilityIds.contains(capability.getId())) {
                        continue;
                    }
                    capabilities.add(capability);
                    capabilityIds.add(capability.getId());
                }
            }
        }
        return capabilities;
    }

    public static <E> void ddlToTapDDLEvent(DDLParserType ddlParserType, String ddl, String spilt, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
        DDLType ddlType = DDLFilter.testAndGetType(ddlParserType, ddl);
        if (null == ddlType) {
            return;
        }
        Class<? extends DDLParser<E>> parserClz = (Class<? extends DDLParser<E>>) ddlParserType.getParserClz();
        DDLParser<E> ddlParser = InstanceFactory.bean(parserClz);
        E parseResult = ddlParser.parse(ddl);
        Class<? extends DDLWrapper<E>>[] ddlWrappers = (Class<? extends DDLWrapper<E>>[]) ddlType.getDdlWrappers();
        for (Class<? extends DDLWrapper<E>> ddlWrapper : ddlWrappers) {
            DDLWrapper<E> ddlWrapperBean = ddlWrapper.getConstructor(String.class).newInstance(spilt);
            ddlWrapperBean.wrap(parseResult, tableMap, consumer);
        }
    }
}
