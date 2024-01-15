package io.tapdata.flow.engine.V2.sharecdc.impl;

import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.bson.Document;
import org.junit.Assert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;

public class ShareCdcPDKTaskReaderTest {
    @Nested
    class TestTapEventWrapper{

        @Test
        void testTapEventWrapper(){
            Document document = new Document();
            document.put("fromTable","CLAIM");
            document.put("timestamp",1705056957000L);
            document.put("before",new Object());
            String op="u";
            document.put("op",op);
            Document after = new Document();
            String claimId="CL_000000001";
            String policyId="PC_000000001";
            after.put("CLAIM_ID",claimId);
            after.put("POLICY_ID",policyId);
            document.put("after",after);
            List<String> removeFields=new ArrayList<>();
            removeFields.add("age");
            document.put("removeFields",removeFields);
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                Assert.assertEquals(claimId,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                Assert.assertEquals(policyId,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                List<String> removedFields = tapUpdateRecordEvent.getRemovedFields();
                Assert.assertEquals("age",removedFields.get(0));
            }
        }
        @Test
        void testTapEventWrapperRemoveFieldIsNull(){
            Document document = new Document();
            document.put("fromTable","CLAIM");
            document.put("timestamp",1705056957000L);
            document.put("before",new Object());
            String op="u";
            document.put("op",op);
            Document after = new Document();
            String claimId="CL_000000001";
            String policyId="PC_000000001";
            after.put("CLAIM_ID",claimId);
            after.put("POLICY_ID",policyId);
            document.put("after",after);
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                when(PdkUtil.decodeOffset(anyString(),any(ConnectorNode.class))).thenReturn("123");
                ShareCdcPDKTaskReader shareCdcPDKTaskReader = spy(new ShareCdcPDKTaskReader(new ConcurrentHashMap<>()));
                ShareCdcContext shareCdcContext = mock(ShareCdcTaskPdkContext.class);
                shareCdcPDKTaskReader.shareCdcContext=shareCdcContext;
                ShareCdcBaseReader.ShareCDCReaderEvent shareCDCReaderEvent = shareCdcPDKTaskReader.tapEventWrapper(document);
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) shareCDCReaderEvent.getTapEvent();
                Assert.assertEquals(claimId,tapUpdateRecordEvent.getAfter().get("CLAIM_ID"));
                Assert.assertEquals(policyId,tapUpdateRecordEvent.getAfter().get("POLICY_ID"));
                List<String> removedFields = tapUpdateRecordEvent.getRemovedFields();
                Assert.assertNull(removedFields);
            }
        }
    }
}
