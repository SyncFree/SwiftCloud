package org.uminho.gsd.benchmarks.helpers;

import java.util.HashMap;
import java.util.Map;

public class RiakGenericObject {

	private Map<String, Object> attributes;
	
	public RiakGenericObject(){
		attributes = new HashMap<String, Object>();
	}
	
	public void setAttribute(String attributeName, Object value){
		attributes.put(attributeName, value);
	}
	
	public Object getAttribute(String attributeName){
		return attributes.get(attributeName);
		
	}
	
}
