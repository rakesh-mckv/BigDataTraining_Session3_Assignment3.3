package onidacount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnidaStateReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text statename, Iterable<IntWritable> values, Context context)
				throws IOException,InterruptedException{
		
		int sum = 0;
		for(IntWritable value : values){
			sum+= value.get();
		}
		context.write(statename, new IntWritable(sum));
	}

}
