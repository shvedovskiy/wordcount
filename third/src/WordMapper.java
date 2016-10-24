import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class WordMapper
extends Mapper<Object, Text, Text, Text>
{
	private static final Pattern WORD_PATTERN = Pattern.compile("\\w+");

	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
		final Matcher matcher = WORD_PATTERN.matcher(value.toString());
		if (matcher.find()) {
			String previous = matcher.group();
			while (matcher.find()) {
				String next = matcher.group();
				context.write(new Text(previous), new Text(next));
				previous = next;
			}
		}
	}
}
