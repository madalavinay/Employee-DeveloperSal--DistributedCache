import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmployeeReducer extends Reducer<Text,LongWritable,Text,LongWritable>
{
	@Override
	public void reduce(Text key,Iterable<LongWritable> values,Context c) throws IOException,InterruptedException
	{
		long sum=0;
		for (LongWritable sal : values) {
			sum+=sal.get();
		}
		c.write(key, new LongWritable(sum));
	}
}
