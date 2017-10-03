package onidacount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class OnidaStateMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	public void map(LongWritable key, Text value,Context context) 
				throws IOException,InterruptedException{
		
		String[] LineArray = value.toString().split("\\|");
		
		if(LineArray[0].equals("Onida")){
			Text statename = new Text(LineArray[3]);
			IntWritable unit = new IntWritable(1);
			context.write(statename, unit);
		}
	}
}
