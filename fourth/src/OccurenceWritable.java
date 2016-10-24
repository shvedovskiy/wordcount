import org.apache.hadoop.io.*;

import java.io.*;

public class OccurenceWritable implements Writable {
	private String mString;
	private long mOccurences;

	public OccurenceWritable() {

	}

	public OccurenceWritable(String string, long occurences) {
		mString = string;
		mOccurences = occurences;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(mString);
		out.writeLong(mOccurences);
	}

	public void readFields(DataInput in) throws IOException {
		mString = in.readUTF();
		mOccurences = in.readLong();
	}

	@Override
	public String toString() {
		return mString + " " + String.valueOf(mOccurences);
	}
} 
