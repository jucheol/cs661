package cyclients.spj;

/**
 * DBRel stores database relations
 */
public class DbRel {
	private String relName;
	private int numOfAttributes;
	
	/**
	 * Creates a DBRel
	 *
	 * @param  relName relation name
	 * @param  numOfAttributes the number of attributes
	 * 
	 */
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
