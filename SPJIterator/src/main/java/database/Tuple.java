package database;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tuple contains data of all attributes
 */
public class Tuple {
	private Map<String, byte[]> attrData; 
	
	/**
	 * Creates a tuple
	 *
	 * @param  dbAttr database attributes
	 * @param  buffer an array of bytes which corresponds a tuple
	 * 
	 */
	public Tuple(List<DbAttr> dbAttr, byte[] buffer) {
		attrData = new HashMap<String, byte[]>();
		int offset = 0;
		for (DbAttr attr : dbAttr) {			
			attrData.put(attr.getAttrName(), Arrays.copyOfRange(buffer, offset, offset + attr.getAttrLength()));
			offset += attr.getAttrLength();
		}
	}
	/**
	 * constructs new tuple object using two tuples and key
	 * attribute key's value will be tuple b's value
	 * @param a
	 * @param b
	 * @param key
	 */
	public Tuple (Tuple a, Tuple b, String key) {
		attrData = a.getMap();
		attrData.putAll(b.getMap());		
	}
	
	public boolean contains(String attr){
		return attrData.containsKey(attr);
	}
	
	public Map<String, byte[]> getMap(){
		return this.attrData;
	}
	
	public byte[] getData(String attrName) {
		if (attrData.containsKey(attrName)) {
			return attrData.get(attrName);
		}
		else {
			return null;
		}
	}
	
	public boolean join(String key, Tuple tuple) {
//		System.out.println(new String(tuple.getData(key)));
//		System.out.println(new String(this.getData(key)));
		return Arrays.equals(tuple.getData(key), this.getData(key));
	}
}
