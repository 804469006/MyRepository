package sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<KeyBean, Text>{

	@Override
	public int getPartition(KeyBean key, Text value, int numPartitions) {
		
		return (key.getLeft().hashCode()* 127) % numPartitions;
	}

}
