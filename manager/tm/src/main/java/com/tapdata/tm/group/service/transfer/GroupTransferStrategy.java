package com.tapdata.tm.group.service.transfer;

import org.bson.types.ObjectId;

import java.io.IOException;

public interface GroupTransferStrategy {
    GroupTransferType getType();

    void exportGroups(GroupExportRequest request);

	default ObjectId importGroups(GroupImportRequest request) throws IOException {
		throw new UnsupportedOperationException();
	}
}
