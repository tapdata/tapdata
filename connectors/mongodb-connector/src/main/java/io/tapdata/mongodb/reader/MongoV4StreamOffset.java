package io.tapdata.mongodb.reader;

import io.tapdata.mongodb.MongoStreamOffset;
import org.bson.BsonDocument;

/**
 * @author jackin
 * @date 2022/5/23 21:06
 **/
public class MongoV4StreamOffset implements MongoStreamOffset {

		private BsonDocument resumeToken;

		public BsonDocument getResumeToken() {
				return resumeToken;
		}

		public void setResumeToken(BsonDocument resumeToken) {
				this.resumeToken = resumeToken;
		}

		@Override
		public String toString() {
				final StringBuilder sb = new StringBuilder("MongoV4StreamOffset{");
				sb.append("resumeToken=").append(resumeToken);
				sb.append('}');
				return sb.toString();
		}
}
