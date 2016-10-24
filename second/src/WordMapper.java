import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class WordMapper
extends Mapper<Object, Text, IntWritable, IntWritable>
{
	private static final Pattern WORD_PATTERN = Pattern.compile("\\w+");
	private static final IntWritable ONE = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final Matcher matcher = WORD_PATTERN.matcher(value.toString());
		while (matcher.find()) {
			context.write(new IntWritable(matcher.group().length()), ONE);
		}
	}
}
