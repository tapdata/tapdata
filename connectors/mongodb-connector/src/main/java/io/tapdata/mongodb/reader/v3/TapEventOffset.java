package io.tapdata.mongodb.reader.v3;

import io.tapdata.entity.event.TapEvent;

/**
 * @author jackin
 * @date 2022/5/23 22:06
 **/
public class TapEventOffset {

		private TapEvent tapEvent;

		private MongoV3StreamOffset offset;

		private String replicaSetName;

		public TapEventOffset(TapEvent tapEvent, MongoV3StreamOffset offset, String replicaSetName) {
				this.tapEvent = tapEvent;
				this.offset = offset;
				this.replicaSetName = replicaSetName;
		}

		public TapEvent getTapEvent() {
				return tapEvent;
		}

		public MongoV3StreamOffset getOffset() {
				return offset;
		}

		public String getReplicaSetName() {
				return replicaSetName;
		}

		@Override
		public String toString() {
				final StringBuilder sb = new StringBuilder("TapEventOffset{");
				sb.append("tapEvent=").append(tapEvent);
				sb.append(", offset=").append(offset);
				sb.append(", replicaSetName='").append(replicaSetName).append('\'');
				sb.append('}');
				return sb.toString();
		}
}
