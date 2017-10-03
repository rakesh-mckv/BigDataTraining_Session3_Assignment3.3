package tvunit1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class TotalUnitMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	Text tvname;
	//IntWritable unit;
	
	public void setup(Context context){
		
		tvname = new Text();
		//  unit = new IntWritable();
	}	
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException{
		
		String[] lineArray2 = value.toString().split("\\|");
		
		if(!(lineArray2[0].equals("NA") || (lineArray2[1].equals("NA")))){
			    tvname.set((lineArray2[0]));
				IntWritable unit = new IntWritable(1);
				context.write(tvname,unit);
			}		
	}
}
