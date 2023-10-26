package io.tapdata.common.ddl;

import io.tapdata.common.ddl.parser.DDLParser;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.type.DDLType;
import io.tapdata.common.ddl.type.WrapperType;
import io.tapdata.common.ddl.wrapper.DDLWrapper;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.Capability;
import net.sf.jsqlparser.statement.alter.Alter;

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
        Class<? extends WrapperType> wrapperType = ddlParserType.getWrapperTypeClass();
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

    public static <E> void ddlToTapDDLEvent(DDLParserType ddlParserType, String ddl, DDLWrapperConfig config, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable {
        ddlToTapDDLEvent(ddlParserType, ddl, config, tableMap, consumer, null);
    }

    public static <E> void ddlToTapDDLEvent(DDLParserType ddlParserType, String ddl, DDLWrapperConfig config, KVReadOnlyMap<TapTable> tableMap,  Consumer<TapDDLEvent> consumer, TapDDLEventFilter tapDDLEventFilter) throws Throwable {
        if (EmptyKit.isBlank(ddl)) {
            return;
        }
        String formatDDL = StringKit.removeLastReturn(ddl.trim());
        Class<? extends DDLFilter> filterClz = ddlParserType.getFilterClass();
        DDLFilter filter = InstanceFactory.bean(filterClz);
        DDLType ddlType = filter.testAndGetType(ddlParserType, formatDDL);
        if (null == ddlType) {
            TapLogger.warn("DDLFactory", "DDLParser not supported: [{}]", formatDDL);
            return;
        }
        formatDDL = filter.filterDDL(ddlType, formatDDL);
        Class<? extends DDLParser<E>> parserClz = (Class<? extends DDLParser<E>>) ddlParserType.getParserClz();
        DDLParser<E> ddlParser = InstanceFactory.bean(parserClz);
        E parseResult = ddlParser.parse(formatDDL);
        Class<? extends DDLWrapper<E>>[] ddlWrappers = (Class<? extends DDLWrapper<E>>[]) ddlType.getDdlWrappers();
        for (Class<? extends DDLWrapper<E>> ddlWrapper : ddlWrappers) {
            DDLWrapper<E> ddlWrapperBean = InstanceFactory.bean(ddlWrapper);
            ddlWrapperBean.init(config);
            if (null == tapDDLEventFilter || tapDDLEventFilter.filter((Alter)parseResult, ddlWrapperBean)) {
                ddlWrapperBean.wrap(parseResult, tableMap, consumer);
            }
        }
    }

    public interface TapDDLEventFilter {
        boolean filter(Alter parseResult, DDLWrapper<?> wrapper);
    }
}
