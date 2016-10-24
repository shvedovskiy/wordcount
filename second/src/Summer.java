import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class Summer
extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
	throws IOException, InterruptedException
	{
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
