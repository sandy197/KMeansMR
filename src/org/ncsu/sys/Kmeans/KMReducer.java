package org.ncsu.sys.Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
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

	private static final boolean DEBUG = true;
	public static String ZK_ADDRESS = "10.1.255.13:2181";
	private int dimension;
	private int k;
	private int R1;
	private boolean isCbuilt, isVbuilt;
	private List<Value> centroids, vectors;
	
	public void setup (Context context) {
		init(context);
	}
	
	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("KM.dimension", 2);
		k = conf.getInt("KM.k", 6);
		R1 = conf.getInt("KM.R1", 2);
		centroids = new ArrayList<Value>(R1);
		isCbuilt = isVbuilt = false;
	}

	public void reduce(Key _key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {
		//populate clusters and data
		Value[] partialCentroids = null;
		
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
			try{
				partialCentroids = classify(vectors, centroids);
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
			
			//TODO: check the case where a cluster doesn't contain any points. Refer to the fix pointed out on the site.
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			int taskId = context.getTaskAttemptID().getTaskID().getId();
			//TODO: write the partial centroids to files
			Path path = new Path(conf.get("KM.tempClusterDir" + "/" + taskId));
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				      IntWritable.class, Value.class,
				      SequenceFile.CompressionType.NONE);
			for(int i = 0; i < partialCentroids.length ; i++){
				Value centroid = (Value)centroids.get(i);
				IntWritable el = new IntWritable();
				if(partialCentroids[i] != null){
					el.set(partialCentroids[i].getCentroidIdx());
					writer.append(el, partialCentroids[i]);
				}
			}
			writer.close();
			//sync
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
				//TODO : add partial centers of task 0 to the hashmap.
				Hashtable<Integer, Value> auxCentroids = new Hashtable<Integer, Value>();
				for(int i = 0; i < partialCentroids.length; i++){
					auxCentroids.put(partialCentroids[i].getCentroidIdx(), partialCentroids[i]);
				}
				
				//add partial centers of other tasks to the hashmap
				for(int i = 1; i < R1; i++){
					//configureWithClusterInfo for reading a file
					path = new Path(conf.get("KM.tempClusterDir") + "/" + R1);
					Path filePath = fs.makeQualified(path);
					List<Value> partCentroidsFromFile = KMUtils.getCentroidsFromFile(filePath);
					for(Value partialCentroid : partCentroidsFromFile){
						if(auxCentroids.containsKey(partialCentroid.getCentroidIdx())){
							//TODO: clarify changes to count and consider corner cases
							auxCentroids.get(partialCentroid.getCentroidIdx()).addVector(partialCentroid);
						}
						else{
							auxCentroids.put(partialCentroid.getCentroidIdx(), partialCentroid);
						}
					}
				}
				
				int centroidCount = auxCentroids.keySet().size();
				Hashtable<Integer, Value> centroidsMap = new Hashtable<Integer, Value>(); 
				
				//need to identify which centroid has not been assigned any points/vectors
				HashSet<Integer> centroidIndices = new HashSet<Integer>();
				for(Value centroid : centroids){
					centroidsMap.put(centroid.getCentroidIdx(), centroid);
					centroidIndices.add(centroid.getCentroidIdx());
				}
				
				for(Integer key : auxCentroids.keySet()){
					//TODO: compute new clusters
					Value newCentroid = computeNewCentroid(auxCentroids.get(key));
					newCentroid.setCentroidIdx(key);
					// write it back as a standard reducer output !
					// make sure all the k-cluster centroids are written even the ones with size-zero
					if(newCentroid != null && centroidIndices.contains(key)){
						context.write(new Key(key, VectorType.CENTROID), newCentroid);
						centroidIndices.remove(key);
					}
					else{
						if(newCentroid != null)
							throw new InterruptedException("FATAL: multiple centroids with same id !!");
					}
				}
				
				if(!centroidIndices.isEmpty()){
					for(Integer key : centroidIndices) {
						context.write(new Key(key, VectorType.CENTROID), centroidsMap.get(key));
					}
				}
			}
		}
	}

	private Value computeNewCentroid(Value value) {
		if(value.getCount() == 0)
			return null;
		else {
			Value newCentroid = new Value(value.getDimension());
			int[] coords = value.getCoordinates();
			int[] newCoords = newCentroid.getCoordinates();
			for(int i = 0; i < coords.length; i++){
				newCoords[i] = coords[i]/value.getCount();
			}
			return newCentroid;
		}
			
	}

	
	private Value[] classify(List<Value> vectors2, List<Value> centroids2) throws Exception {
		Value[] partialCentroids = new Value[centroids2.size()];
		for(Value point : vectors2){
			int idx = getNearestCentroidIndex(point, centroids2);
			if(partialCentroids[idx] == null){
				partialCentroids[idx] = new Value(point.getDimension());
			}
			partialCentroids[idx].addVector(point);
			if(partialCentroids[idx].getCentroidIdx() == -1){
				partialCentroids[idx].setCentroidIdx(idx);
			}
			else {
				if(partialCentroids[idx].getCentroidIdx() != idx)
					if(DEBUG) throw new Exception("Fatal: Inconsistent cluster, multiple centroids problem!");
			}
		}
		return partialCentroids;
	}

	private int getNearestCentroidIndex(Value point, List<Value> centroids2) {
		int nearestCidx = -1;
		int shortestDistance = Integer.MAX_VALUE;
		for(Value centroid : centroids2){
			int distance = KMUtils.getDistance(point.getCoordinates(), centroid.getCoordinates());
			if(distance < shortestDistance){
				nearestCidx  = centroid.getCentroidIdx();
				shortestDistance = distance;
			}
		}
		return nearestCidx;
		
	}

	private void buildCentroids(Iterable<Value> values, List<Value> centroidsLoc) {
		for(Value val : values){
			centroidsLoc.add(val);
		}
		
	}

}
