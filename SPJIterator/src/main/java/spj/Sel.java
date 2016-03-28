package spj;
/**
 * 
 * @author Daolin Cheng
 * Class for selection condition
 *
 */
public class Sel {
	DbAttr attr;
	byte[] data;
	public Sel(DbAttr attr, byte[] data) {
		this.attr = attr;
		this.data = data;
	}
	
	public boolean satisfiedBy(Tuple t){
		//did not consider data type match
		return t.contains(attr.getAttrName()) 
				&& data.equals(t.getData(attr.getAttrName()));
	}
}
