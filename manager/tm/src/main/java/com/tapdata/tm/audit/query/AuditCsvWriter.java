package com.tapdata.tm.audit.query;

import com.tapdata.tm.audit.AuditBatch;
import com.tapdata.tm.audit.AuditChange;
import com.tapdata.tm.utils.MessageUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Writer;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** CSV 导出（原始事件粒度）：含 Event ID 与 Operation ID；防止以 = + - @ 开头的单元格被表格软件当作公式执行。 */
public class AuditCsvWriter {
	static final List<String> HEADERS = List.of("event_id", "operation_id", "phase", "timestamp", "event_type", "action", "action_name",
			"outcome", "user_id", "username", "source", "source_ip", "source_ip_note", "object_type", "object_id", "object_name",
			"reason", "detail_level", "summary", "changes", "batch", "service_node", "legacy_message");

	private final Writer writer;
	private final DateTimeFormatter formatter;
	private final Locale locale;

	public AuditCsvWriter(Writer writer, ZoneId zone, Locale locale) {
		this.writer = writer;
		this.formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(zone);
		this.locale = locale;
	}

	public void header() throws IOException {
		writer.write('﻿');
		line(HEADERS);
	}

	public void row(AuditEventVo e) throws IOException {
		line(List.of(s(e.getEventId()), s(e.getOperationId()), s(e.getPhase()),
				e.getTimestamp() == null ? "" : formatter.format(e.getTimestamp().toInstant()),
				s(e.getEventType()), s(e.getAction()), s(e.getActionName()), s(e.getOutcome()), s(e.getUserId()), s(e.getUsername()),
				s(e.getSource()), s(e.getSourceIp()), s(message(e.getSourceIpNote())), s(e.getObjectType()), s(e.getObjectId()),
				s(e.getObjectName()), s(message(e.getReason())), s(e.getDetailLevel()), s(message(e.getSummary())), changes(e.getChanges()),
				batch(e.getBatch()), e.getNode() == null ? "" : StringUtils.joinWith(" ", s(e.getNode().getService()), s(e.getNode().getHost()), s(e.getNode().getIp())).trim(),
				s(e.getLegacyMessage())));
	}

	private String message(String key) {
		if (key == null) {
			return null;
		}
		String text = MessageUtil.getBundleMessageOrNull(locale, "auditMessages", key);
		return text != null ? text : key;
	}

	private String changes(List<AuditChange> changes) {
		if (changes == null || changes.isEmpty()) {
			return "";
		}
		return changes.stream().map(c -> c.getSecretState() != null
				? c.getField() + ": " + message("audit_value_" + c.getSecretState().toLowerCase(Locale.ROOT))
				: c.getField() + ": " + s(c.getBefore()) + " -> " + s(c.getAfter())).collect(Collectors.joining("; "));
	}

	private static String batch(AuditBatch batch) {
		if (batch == null) {
			return "";
		}
		String failed = batch.getItems().stream().filter(i -> i.getOutcome() != null && "FAILED".equals(i.getOutcome().name()))
				.map(i -> StringUtils.defaultIfBlank(i.getObjectName(), i.getObjectId()) + (i.getReason() == null ? "" : "(" + i.getReason() + ")"))
				.collect(Collectors.joining(", "));
		return "total=" + batch.getTotal() + " success=" + batch.getSuccess() + " failed=" + batch.getFailed()
				+ (failed.isEmpty() ? "" : " failed_items=" + failed);
	}

	private void line(List<String> cells) throws IOException {
		writer.write(cells.stream().map(AuditCsvWriter::cell).collect(Collectors.joining(",")));
		writer.write("\r\n");
	}

	static String cell(String value) {
		String v = value == null ? "" : value;
		if (!v.isEmpty() && "=+-@\t\r".indexOf(v.charAt(0)) >= 0) {
			v = "'" + v;
		}
		if (v.contains(",") || v.contains("\"") || v.contains("\n") || v.contains("\r")) {
			v = "\"" + v.replace("\"", "\"\"") + "\"";
		}
		return v;
	}

	private static String s(String value) {
		return value == null ? "" : value;
	}
}
