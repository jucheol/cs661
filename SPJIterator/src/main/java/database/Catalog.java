package database;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * Catalog parses the xml file
 */
public class Catalog {
	private Document xml;

	/**
	 * Creates a catalog
	 *
	 * @param  catalogFile the xml file which contains catalog information
	 * @throws Exception IO exception
	 * 
	 */
	public Catalog(File catalogFile) throws Exception {
		SAXReader reader = new SAXReader();
		xml = reader.read(catalogFile);
	}

	public List<DbRel> getDbRels() {
		List<DbRel> rels = new LinkedList<DbRel>();
		for (Object item : xml.selectNodes("/Relational_Db/dbRel")) {
			if (item instanceof Element) {
				Element itemElement = (Element) item;
				DbRel rel = new DbRel(itemElement.attributeValue("relName"), itemElement.attributeValue("numOfAttributes"));
				rels.add(rel);
			}
		}
		return rels;
	}

	public List<DbAttr> getDbAttrs(String relName) {
		List<DbAttr> attrs = new LinkedList<DbAttr>();
		for (Object item : xml.selectNodes("/Relational_Db/dbRel[@relName='" + relName + "']/dbAttr")) {
			if (item instanceof Element) {
				Element itemElement = (Element) item;
				DbAttr attr = new DbAttr(itemElement.attributeValue("attrName")
						, itemElement.attributeValue("attrType")
						, itemElement.attributeValue("attrLength"));
				attrs.add(attr);
			}
		}
		return attrs;
	}
	
	public int getTupleLength(String relName) {
		int length = 0;
		for (Object item : xml.selectNodes("/Relational_Db/dbRel[@relName='" + relName + "']/dbAttr")) {
			if (item instanceof Element) {
				Element itemElement = (Element) item;
				length += Integer.parseInt(itemElement.attributeValue("attrLength"));
			}
		}
		return length;
	}

	public static void main(String args[]) throws Exception {
		Catalog test = new Catalog(new File("SPJIterator/data/xmlCatalog.xml"));
		System.out.println(test.getDbRels());
		System.out.println(test.getDbAttrs(test.getDbRels().get(0).getRelName()));
	}
}