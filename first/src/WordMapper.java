import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class WordMapper
extends Mapper<Object, Text, Text, IntWritable>
{
    public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException
	{
        StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r.,/?!;:-\'\"\\", false);
        while (itr.hasMoreTokens()) {
            context.write(new Text(itr.nextToken()), new IntWritable(1));
        }
    }
}
