import org.apache.hadoop.io.*;

import java.io.*;

public class OccurenceWritable implements Writable {
	private String mString;
	private long mOccurences;

	public OccurenceWritable() {
	}
	public OccurenceWritable(String string, long occurences) {
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
	@Override
	public String toString() {
		return this.mString + "\t" + String.valueOf(this.mOccurences);
	}
} 
