package org.ncsu.sys.KmeansTest;

import java.util.ArrayList;
import java.util.List;

import org.ncsu.sys.Kmeans.KMTypes.Value;

public class IteratorTest {
	
	
	List<Value> vals = new ArrayList<Value>();
	
	public IteratorTest(){
		for(int i = 0; i < 10; i++){
			vals.add(new Value(i+1));
		}
	}
	
	public static void main(String[] args){
		List<Value> vals1 = new ArrayList<Value>();
		Value[] vals2 = new Value[10];
		for(int i =0 ; i < 10 ; i++){
			vals2[i].setDimension(i);
		}
	}
}
