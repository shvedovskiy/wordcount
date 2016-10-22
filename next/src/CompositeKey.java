import org.apache.hadoop.io.*;

import java.io.*;

public class CompositeKey implements WritableComparable<CompositeKey> {
	private String mString;
	private long mOccurences;

	public CompositeKey() {
	}
	public CompositeKey(String string, long occurences) {
		this.mString = string;
		this.mOccurences = occurences;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.mString);
		out.writeLong(this.mOccurences);
	}
	public void readFields(DataInput in) throws IOException {
		this.mString = in.readUTF();
		this.mOccurences = in.readLong();
	}
	public String getString() {
		return this.mString;
	}
	public long getOccurences() {
		return this.mOccurences;
	}
	@Override
	public String toString() {
		return this.mString;
	}
	@Override
	public int compareTo(CompositeKey o) {
		final long thisValue = this.mOccurences;
		final long thatValue = o.mOccurences;
		return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
	}
} 
