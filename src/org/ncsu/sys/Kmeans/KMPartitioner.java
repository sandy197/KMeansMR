package org.ncsu.sys.Kmeans;

import org.apache.hadoop.mapreduce.Partitioner;
import org.ncsu.sys.Kmeans.KMTypes.Key;
import org.ncsu.sys.Kmeans.KMTypes.Value;

public class KMPartitioner extends Partitioner<Key, Value> {

	@Override
	public int getPartition(Key key, Value value, int reduceTaskCount) {
		return key.getTaskIndex() % reduceTaskCount;
	}

}
