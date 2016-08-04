import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReportReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		IntWritable max = new IntWritable();
		for (IntWritable value :values){
			if(value.compareTo(max)==1){
				max = value;
			}
		}
		context.write(key, max);
	}
}
