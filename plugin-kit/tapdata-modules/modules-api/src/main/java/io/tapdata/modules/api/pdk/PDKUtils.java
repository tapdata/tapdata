package io.tapdata.modules.api.pdk;

import java.util.Map;

public interface PDKUtils {
    Map<String, Object> getConnectionConfig(String connectionId);
	PDKInfo downloadPdkFileIfNeed(String pdkHash);
	class PDKInfo {
		private String pdkId;
		public PDKInfo pdkId(String pdkId) {
			this.pdkId = pdkId;
			return this;
		}
		private String group;
		public PDKInfo group(String group) {
			this.group = group;
			return this;
		}
		private String version;
		public PDKInfo version(String version) {
			this.version = version;
			return this;
		}

		public String getPdkId() {
			return pdkId;
		}

		public void setPdkId(String pdkId) {
			this.pdkId = pdkId;
		}

		public String getGroup() {
			return group;
		}

		public void setGroup(String group) {
			this.group = group;
		}

		public String getVersion() {
			return version;
		}

		public void setVersion(String version) {
			this.version = version;
		}
	}
}
