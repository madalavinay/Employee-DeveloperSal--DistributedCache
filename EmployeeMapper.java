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
