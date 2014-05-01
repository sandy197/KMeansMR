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
		IteratorTest test = new IteratorTest();
		Iterable<Value> iterable =  (Iterable<Value>) test.vals.iterator();
		for(Value val : iterable){
			vals1.add(val);
		}		
		
		for(Value val : vals1){
			System.out.println(val.getDimension());
		}
	}
}
