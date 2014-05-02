package org.ncsu.sys.Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.zookeeper.KeeperException;
import org.ncsu.sys.Kmeans.KMTypes.Key;
import org.ncsu.sys.Kmeans.KMTypes.Value;
import org.ncsu.sys.Kmeans.KMTypes.VectorType;
import org.ncsu.sys.Kmeans.SyncPrimitive.Barrier;

public class KMReducer extends Reducer<Key, Value, Key, Value> {

	public static String ZK_ADDRESS = "10.1.255.13:2181";
	private int dimension;
	private List<int[]> centroids, vectors;
	private int R1;
	boolean isCbuilt, isVbuilt;
	
	public void setup (Context context) {
		init(context);
	}
	
	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("KM.dimension", 2);
		R1 = conf.getInt("KM.R1", 2);
		centroids = new ArrayList<int[]>(R1);
		isCbuilt = isVbuilt = false;
	}

	public void reduce(Key _key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {
		//populate clusters and data
		Value[] partialCentroids;
		
		if(_key.getType() == VectorType.CENTROID){
			buildCentroids(values, centroids);
			isCbuilt = true;
		}
		else{
			//TODO : use build and set here
			buildCentroids(values, vectors);
			isVbuilt = true;
			//buildVectors(values, vectors);
		}
		//TODO : compute the partial clusters
		if(isCbuilt && isVbuilt){
			partialCentroids = classify(vectors, centroids);
			
			//TODO: check the case where a cluster doesn't contain any points. Refer to the fix pointed out on the site.
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			int taskId = context.getTaskAttemptID().getTaskID().getId();
			//TODO: write the partial centroids to files
			Path path = new Path(conf.get("KM.tempClusterDir" + "/" + taskId));
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				      Value.class, Value.class,
				      SequenceFile.CompressionType.NONE);
			for(int i = 0; i < partialCentroids.length ; i++){
				writer.append(centroids.get(i), partialCentroids[i]);
			}
			writer.close();
			//TODO: sync
			Barrier b = new Barrier(ZK_ADDRESS, "/b1", R1);
			try{
			    boolean flag = b.enter();
			    System.out.println("Entered barrier: " + 6);
			if(!flag) System.out.println("Error when entering the barrier");
			} catch (KeeperException e){
				e.printStackTrace();
			} catch (InterruptedException e){
				e.printStackTrace();
			}
			
			//TODO: read files and compute newClusters only if its the task 0
			if(taskId == 0){
				for(int i = 0; i < R1; i++){
					//TODO:configureWithClusterInfo for raeding a file
					//TODO: compute new clusters by reading all the partial clusters pertaining to a particular centroid.
						//can use hashmap here
					//TODO:write it back as a standard reducer output !
				}
			}
		}
	}

	private Value[] classify(List<int[]> vectors2, List<int[]> centroids2) {
		Value[] partialCentroids = new Value[centroids2.size()];
		for(int[] point : vectors2){
			int idx = getNearestCentroidIndex(point, centroids2);
			if(partialCentroids[idx] == null){
				partialCentroids[idx] = new Value(point.length);
			}
			partialCentroids[idx].addVector(point);
		}
		return partialCentroids;
	}

	private int getNearestCentroidIndex(int[] point, List<int[]> centroids2) {
		int idx = 0;
		int nearestCidx = -1;
		int shortestDistance = Integer.MAX_VALUE;
		for(int[] centroid : centroids2){
			int distance = getDistance(point, centroid);
			if(distance < shortestDistance){
				nearestCidx  = idx;
				shortestDistance = distance;
			}
			idx++;
		}
		return nearestCidx;
		
	}

	private int getDistance(int[] point, int[] centroid) {
		int distance = 0;
		for(int i = 0; i < point.length; i++){
			distance += (point[i] - centroid[i]) * (point[i] - centroid[i]); 
		}
		return distance;
	}

	private void buildCentroids(Iterable<Value> values, List<int[]> centroidsLoc) {
		for(Value val : values){
			centroidsLoc.add(val.getCoordinates());
		}
		
	}

}
