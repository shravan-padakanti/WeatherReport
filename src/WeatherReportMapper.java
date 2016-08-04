import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherReportMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{

		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			int year = Integer.parseInt(value.toString().trim().substring(15, 19));
			int length = value.toString().trim().length();
			String temp = value.toString().trim().substring(length-18, length-12);
			
			if (temp.startsWith("+")){
				temp = temp.substring(1);
			}
						
			context.write(new IntWritable(year), new IntWritable(Integer.parseInt(temp)));
			
		}
}
