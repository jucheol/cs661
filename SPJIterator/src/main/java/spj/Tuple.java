package spj;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Tuple {
	private Map<String, byte[]> attrData; 
	
	public Tuple(List<DbAttr> dbAttr, byte[] buffer) {
		attrData = new HashMap<String, byte[]>();
		int offset = 0;
		for (DbAttr attr : dbAttr) {			
			attrData.put(attr.getAttrName(), Arrays.copyOfRange(buffer, offset, offset + attr.getAttrLength()));
			offset += attr.getAttrLength();
		}
	}
	
	public byte[] getData(String attrName) {
		return attrData.get(attrName);
	}	
}
