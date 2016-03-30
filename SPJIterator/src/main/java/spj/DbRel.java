package spj;

public class DbRel {
	private String relName;
	private int numOfAttributes;
	
	public DbRel(String relName, String numOfAttributes) {
		super();
		this.relName = relName;
		this.numOfAttributes = Integer.parseInt(numOfAttributes);
	}
	
	public String getRelName() {
		return relName;
	}
	
	public int getNumOfAttributes() {
		return numOfAttributes;
	}
	


	@Override
	public String toString() {
		return "DbRel [relName=" + relName + ", numOfAttributes=" + numOfAttributes + "]";
	}
}
