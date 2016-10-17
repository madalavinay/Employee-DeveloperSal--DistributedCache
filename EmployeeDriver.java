import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmployeeDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new EmployeeDriver(),
				args);
		System.exit(status);
	}

	@Override
	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException, URISyntaxException {
		Job job = Job.getInstance(getConf(),
				"Employees total salary using Distributed Cache");
		job.setJarByClass(EmployeeDriver.class);

		job.addCacheFile(new Path(args[0]).toUri());

		job.setMapperClass(EmployeeMapper.class);
		job.setReducerClass(EmployeeReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
