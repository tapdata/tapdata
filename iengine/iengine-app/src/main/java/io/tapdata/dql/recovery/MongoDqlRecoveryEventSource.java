package io.tapdata.dql.recovery;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.dql.vo.DqlRecoveryPayloadVo;
import io.tapdata.dql.client.DqlTmClient;
import io.tapdata.dql.model.DqlPayloadSnapshot;

import java.util.Objects;

/** Reads immutable DQL payload snapshots from TM's metadata Mongo database. */
public final class MongoDqlRecoveryEventSource implements DqlRecoveryEventSource {
    private final DqlTmClient tmClient;

    public MongoDqlRecoveryEventSource(ClientMongoOperator clientMongoOperator) {
        ClientMongoOperator operator = Objects.requireNonNull(
                clientMongoOperator, "clientMongoOperator must not be null");
        if (!(operator instanceof HttpClientMongoOperator httpClientMongoOperator)) {
            throw new IllegalArgumentException("DQL recovery payload requires the Engine HTTP Mongo operator");
        }
        this.tmClient = new DqlTmClient(httpClientMongoOperator);
    }

    @Override
    public DqlPayloadSnapshot load(String eventId) {
        return loadEvent(eventId).payload();
    }

    @Override
    public DqlRecoveryEvent loadEvent(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("DQL eventId must not be blank");
        }
        DqlRecoveryPayloadVo event = tmClient.getRecoveryPayload(eventId);
        DqlPayloadSnapshot snapshot = new DqlPayloadSnapshot();
        snapshot.setPayloadFormat(event.getPayloadFormat());
        snapshot.setPayloadData(event.getPayloadData());
        snapshot.setPayloadHash(event.getPayloadHash());
        snapshot.setPayloadSize(event.getPayloadSize());
        snapshot.setPayloadComplete(event.getPayloadComplete());
        snapshot.setPayloadPreview(event.getPayloadPreview());
        snapshot.setPayloadPreviewTruncated(event.getPayloadPreviewTruncated());
        return new DqlRecoveryEvent(snapshot,
                event.getSourceNodeId(), event.getSourceNodeName(),
                event.getFailedNodeId(), event.getFailedNodeName(),
                event.getTargetNodeId(), event.getTargetNodeName());
    }
}
