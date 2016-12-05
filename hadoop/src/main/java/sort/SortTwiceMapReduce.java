package sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortTwiceMapReduce {

	public static void main(String[] args) throws Exception {
		// get Job
		Job job = Job.getInstance(new Configuration());

		// set Jar
		job.setJarByClass(SortTwiceMapReduce.class);

		// set Mapper
		job.setMapperClass(SortTwiceMapper.class);
		job.setMapOutputKeyClass(KeyBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setGroupingComparatorClass(MyGroupingComparator.class);

		// set Reducer
		job.setReducerClass(SortTwiceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// PathIn PathOut
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// submit
		job.waitForCompletion(true);
	}

	public static class SortTwiceMapper extends
			Mapper<LongWritable, Text, KeyBean, Text> {
		KeyBean kBean = new KeyBean();
		Text textV = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			kBean.set(split[0], Integer.valueOf(split[1]));
			textV.set(split[1]);
			context.write(kBean, textV);
		}
	}

	public static class SortTwiceReducer extends
			Reducer<KeyBean, Text, Text, Text> {
		Text key = new Text();

		@Override
		protected void reduce(KeyBean keyBean, Iterable<Text> iterable,
				Context context) throws IOException, InterruptedException {

			System.out.println(keyBean);
			for (Text text : iterable) {
				key.set(keyBean.getLeft());
				context.write(key, text);
			}
		}
	}
}
