import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class REC1 {
	static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String v = value.toString();
			String[] s1 = v.split(":");
			String[] s2 = s1[1].split(",");
			for (int i = 0; i < s2.length; i++) {
				for (int j = i+1; j < s2.length; j++) {
					context.write(new Text(s2[i]+"-"+s2[j]), new IntWritable(1));
					context.write(new Text(s2[j]+"-"+s2[i]), new IntWritable(1));
				}
			}
		}
	}
	
	static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text kin, Iterable<IntWritable> vin,
				Context context) throws IOException, InterruptedException {
			int sum =0;
			for (IntWritable in : vin) {
				sum =sum +in.get();	
			}
			context.write(kin, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job =new Job(conf);
		job.setJarByClass(REC1.class);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job,arg1);
		FileOutputFormat.setOutputPath(job, new Path(arg2));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
