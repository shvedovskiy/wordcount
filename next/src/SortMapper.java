import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SortMapper extends Mapper<Object, Text, CompositeKey, OccurenceWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("[\\r\\n]+");
		for (String line : lines) {
			String[] words = line.split("\t");
			if (words.length != 3) {
				continue;
			}
			String keyString = words[0];
			String valueString = words[1];
			long count = Long.parseLong(words[2]);
			context.write(new CompositeKey(keyString, count), new OccurenceWritable(valueString, count));
		}
	}
}
