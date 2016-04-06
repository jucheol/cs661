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
	
	public boolean contains(String attr){
		return attrData.containsKey(attr);
	}
	
	public byte[] getData(String attrName) {
		if (attrData.containsKey(attrName)) {
			return attrData.get(attrName);
		}
		else {
			return null;
		}
	}
	
	public boolean join(DbAttr attr, Tuple tuple) {
		return (tuple.getData(attr.getAttrName()).equals(this.getData(attr.getAttrName())));
	}
}
