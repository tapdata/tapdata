package io.tapdata.zoho.service.zoho.schemaLoader;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.ProductsOpenApi;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.*;
import java.util.function.BiConsumer;

public class ProductsSchema extends Schema implements SchemaLoader {
    private ProductsOpenApi productsOpenApi;
    @Override
    public SchemaLoader configSchema(TapConnectionContext tapConnectionContext) {
        this.productsOpenApi = ProductsOpenApi.create(tapConnectionContext);
        return this;
    }

    @Override
    public void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {

    }

    @Override
    public void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer) {
        this.read(batchCount,offset,consumer,Boolean.TRUE);
    }

    @Override
    public long batchCount() throws Throwable {
        return 0;
    }

    public void read(int readSize, Object offsetState, BiConsumer<List<TapEvent>, Object> consumer,boolean isStreamRead ){
        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
        int pageSize = Math.min(readSize, ProductsOpenApi.MAX_PAGE_LIMIT);
        int fromPageIndex = 1;//从第几个工单开始分页
        TapConnectionContext context = this.productsOpenApi.getContext();
        String modeName = context.getConnectionConfig().getString("connectionMode");
        ConnectionMode connectionMode = ConnectionMode.getInstanceByName(context, modeName);
        if (null == connectionMode){
            throw new CoreException("Connection Mode is not empty or not null.");
        }
        String tableName =  Schemas.Products.getTableName();
        if (Checker.isEmpty(offsetState)) offsetState = ZoHoOffset.create(new HashMap<>());
        final Object offset = offsetState;
        final String sortBy = !isStreamRead ? ProductsOpenApi.SortBy.CREATED_TIME.descSortBy(): ProductsOpenApi.SortBy.MODIFIED_TIME.descSortBy();
        while (isAlive()){
            List<Map<String, Object>> list = productsOpenApi.page(fromPageIndex, pageSize, sortBy);
            if (Checker.isNotEmpty(list) && !list.isEmpty()){
                fromPageIndex += pageSize;
                list.stream().filter(Objects::nonNull).forEach(product->{
                    if (!isAlive()) return;
                    Map<String, Object> oneProduct = connectionMode.attributeAssignment(product,tableName,productsOpenApi);
                    if (Checker.isNotEmpty(oneProduct) && !oneProduct.isEmpty()){
                        Object modifiedTimeObj = isStreamRead?null:oneProduct.get("modifiedTime");//stream read is sort by "modifiedTime",batch read is sort by "createdTime"
                        long referenceTime = System.currentTimeMillis();
                        if (Checker.isNotEmpty(modifiedTimeObj) && modifiedTimeObj instanceof String) {
                            referenceTime = this.parseZoHoDatetime((String) modifiedTimeObj);
                            ((ZoHoOffset) offset).getTableUpdateTimeMap().put(tableName, referenceTime);
                        }
                        events[0].add(( TapSimplify.insertRecordEvent(oneProduct, tableName).referenceTime(referenceTime) ));
                        if (events[0].size() == readSize){
                            consumer.accept(events[0], offset);
                            events[0] = new ArrayList<>();
                        }
                    }
                });
                if (!events[0].isEmpty()){
                    consumer.accept(events[0], offsetState);
                }
            }else {
                break;
            }
        }
    }

}
