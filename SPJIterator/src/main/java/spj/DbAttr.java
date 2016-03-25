package spj;

public class DbAttr {
	private String attrName, attrType;
	private int attrLength;

	public DbAttr(String attrName, String attrType, String attrLength) {
		super();
		this.attrName = attrName;
		this.attrType = attrType;
		this.attrLength = Integer.parseInt(attrLength);
	}

	public String getAttrName() {
		return attrName;
	}

	public String getAttrType() {
		return attrType;
	}

	public int getAttrLength() {
		return attrLength;
	}

	@Override
	public String toString() {
		return "DbAttr [attrName=" + attrName + ", attrType=" + attrType + ", attrLength=" + attrLength + "]";
	}
}
