package io.tapdata.mongodb.reader.v3;

import io.tapdata.mongodb.MongoStreamOffset;

/**
 * @author jackin
 * @date 2022/5/23 21:06
 **/
public class MongoV3StreamOffset implements MongoStreamOffset {

		private int seconds;

		private int inc;

		public MongoV3StreamOffset(int seconds, int inc) {
				this.seconds = seconds;
				this.inc = inc;
		}

		public int getSeconds() {
				return seconds;
		}

		public void setSeconds(int seconds) {
				this.seconds = seconds;
		}

		public int getInc() {
				return inc;
		}

		public void setInc(int inc) {
				this.inc = inc;
		}

		@Override
		public String toString() {
				final StringBuilder sb = new StringBuilder("MongoV3StreamOffset{");
				sb.append("seconds=").append(seconds);
				sb.append(", inc=").append(inc);
				sb.append('}');
				return sb.toString();
		}
}
