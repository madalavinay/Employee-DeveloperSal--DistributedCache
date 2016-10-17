//Employee Mapper (Distributed Cache)
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeMapper extends Mapper<Object,Text,Text,LongWritable>{
	public Map<String,String> employeeReference=new HashMap<String,String>();
	
	@Override
	public void setup(Context c) throws IOException, InterruptedException
	{
		Configuration conf=c.getConfiguration();
		FileSystem fs=FileSystem.get(conf);
		
		URI[] uris=c.getCacheFiles();
		Path p=new Path(uris[0].getPath());
		
		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(p)));
		String line=null;
		while((line=reader.readLine())!=null)
		{
			String[] data=line.split(",");
			employeeReference.put(data[0], data[1]);			
		}
	}
	
	Text role=new Text("developer");
	LongWritable salary=new LongWritable();
	@Override
	public void map(Object key,Text value,Context c) throws IOException,InterruptedException
	{
		String[] employeeData=value.toString().replaceAll(" ", ",").split(",");
		if(employeeReference.containsKey(employeeData[0]))
		{
			if(employeeData[employeeData.length-1].equalsIgnoreCase("developer"))
			{
				salary.set(Long.parseLong(employeeData[2]+employeeData[3]));
				c.write(role, salary);
			}
		}
		
	}
	
}

//Employee Reducer
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

//Employee Driver(using ToolRunner)
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

Input Files::
employee.txt:
1,joe,75,000,developer
2,jack,76,000,developer
3,jim,89,000,manager
4,jill,99,000,director
5,chris,88,000,developer
6,ryan,92,000,manager
7,tom,77,000,admin
8,tim,88,000 developer
9,john,56,000,developer
10,james,78,000,developer

employee_reference.txt:
1,joe
4,jill
5,chris
6,ryan
8,tim
7,tom
9,john
10,james

Execution:
hadoop jar /home/cloudera/Desktop/EmployeeSalary.jar EmployeeDriver employee_reference.txt employee.txt developersal
