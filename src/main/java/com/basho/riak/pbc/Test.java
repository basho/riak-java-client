package com.basho.riak.pbc;

import java.io.IOException;

public class Test {

	public static void main(String[] args) {
		try{
				RiakClient rc = new RiakClient("localhost",8087);			
				String bucket = "bucket";
				String key = "key";
				
				String val = "test";
				RiakObject ro = new RiakObject(bucket,key,val);
				rc.store(ro);				

				RiakObject[] ros = rc.fetch(bucket, key);
				
				for(RiakObject r : ros){
					System.out.println(r.toString());			 
				}	

			}catch(IOException e){
				e.printStackTrace();
			}
		
	}
	
}
