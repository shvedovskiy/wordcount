import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class Summer
	extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<>();
		for (Text val : values) {
			final String mapKey = val.toString();
			if (map.containsKey(mapKey)) {
				map.put(mapKey, map.get(mapKey) + 1);
			} else {
				map.put(mapKey, 1);
			}
		}
		int max = 0;
		String value = "";
		for (String mapKey : map.keySet()) {
			if (map.get(mapKey) > max) {
				max = map.get(mapKey);
				value = mapKey;
			}
		}
		context.write(key, new Text(value));
	}
}
