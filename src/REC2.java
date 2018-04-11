import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

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




public class REC2 {
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
	
	static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String s =value.toString();
			String[] s1 =s.split("-");
			context.write(new Text(s1[0]),new Text(s1[1]));
		}
	}
	static class Reduce2 extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text kin, Iterable<Text> vin, Context context)
				throws IOException, InterruptedException {
			SortedMap<Integer, String> map =new TreeMap<>();
			int N=2;
			for (Text text : vin) {
				String[] s =text.toString().split("	");
				if (map.containsKey(Integer.parseInt(s[1]))) {
					String ss =map.get(Integer.parseInt(s[1]));
					ss = ss+" "+s[0];
					map.put(Integer.parseInt(s[1]), ss);
				}
				else {
					map.put(Integer.parseInt(s[1]), s[0]);
				}
				if (map.size()>N) {
					map.remove(map.firstKey());
				}
			}
			String ss ="";
			for(String sv:map.values()){
				ss=sv+" "+ss;
			}
			context.write(new Text(kin.toString()+":"), new Text(ss.trim()));
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job1 =new Job(conf);
		job1.setJarByClass(REC1.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job1, arg1);
		FileOutputFormat.setOutputPath(job1, new Path(arg2));
		job1.waitForCompletion(true);
		
		Job job2 =new Job(conf);
		job2.setJarByClass(REC2.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job2, arg2+"/part-r-00000");
		FileOutputFormat.setOutputPath(job2, new Path(arg3));
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
