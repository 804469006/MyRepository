package toblejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerOrderMapReduce {
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		//1.get Job
		Job job = Job.getInstance(configuration);
		
		//2.set Jar
		job.setJarByClass(CustomerOrderMapReduce.class);
		
		//3.set Mapper
		job.setMapperClass(CustomerOrderMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(CustomerOrderBean.class);
		
		//3.5 set Partition
		//job.setPartitionerClass(MyPartitioner.class);
		//set number of Reducer
		//job.setNumReduceTasks(new Integer(args[2]));
		//job.setNumReduceTasks(3);
		
		//3.6 set Combiner
		//job.setCombinerClass(PVReducer.class);
		
		//4.set Reducer
		job.setReducerClass(CustomerOrderReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		//5.PathIn  PathOut
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//6.submit
		job.waitForCompletion(true);
	}
	public static class CustomerOrderMapper extends
			Mapper<LongWritable, Text, LongWritable, CustomerOrderBean> {
		CustomerOrderBean bean = new CustomerOrderBean();
		LongWritable k = new LongWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			if (split.length == 3) {
				bean.set("customer", split[1] + "," + split[2]);
				k.set(Long.valueOf(split[0]));
				context.write(k, bean);
			}
			if (split.length == 4) {
				bean.set("order", split[1] + "," + split[2] + "," + split[3]);
				k.set(Long.valueOf(split[0]));
				context.write(k, bean);
			}
		}

	}

	public static class CustomerOrderReducer extends
			Reducer<LongWritable, CustomerOrderBean, LongWritable, Text> {
		Text v = new Text();
		@Override
		protected void reduce(LongWritable key,
				Iterable<CustomerOrderBean> iterable, Context context)
				throws IOException, InterruptedException {
			String customerStr = null;
			List<String> orderList = new ArrayList<String>();
			for (CustomerOrderBean customerOrderBean : iterable) {
				String fiag = customerOrderBean.getFiag();
				if (fiag.equals("customer")) {
					customerStr = customerOrderBean.getData();
				} else {
					orderList.add(customerOrderBean.getData());
				}

			}
			/**
			 * Join过程
			 */
			for (String orderData : orderList) {
				v.set(customerStr + "," + orderData);
				context.write(key, v);
			}
		}

	}
}
