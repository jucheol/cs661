package iterator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import database.Reader;
import database.Catalog;
import database.Tuple;

/**
 * SP iterator.
 */
public class Buffer {
	private Condition filter;
	private List<Tuple> nextBuffer;
	private Reader reader;
	private int tuppleId, bufSize = 10;
	
	/**
	 * Creates a SP iterator.
	 *
	 * @param  db database file
	 * @param  relName relation name
	 * @param  cata Catalog object
	 * @param  filter Condition object
	 * @throws Exception IO exception
	 */
	public Buffer(File db, String relName, Catalog cata, Condition filter) throws Exception {
		this.filter = filter;		
		reader = new Reader(db, bufSize, cata.getDbAttrs(relName));
	}
	
	public void open() throws Exception {
		tuppleId = 0;
		reader.fillBuffer();		
	}
	
	public void close() {
		System.err.println("SP iterator closed.");
	}
	
	public List<Tuple> getNext() {		
		return nextBuffer;
	}
	
	public boolean hasNext() throws Exception {
		Tuple tp = null;
		List<Tuple> tmp = new ArrayList<Tuple>(bufSize);
		while (tp == null) {
			if (tuppleId == reader.getNumTuples()) {
				if (reader.hasTuple()) {
					reader.fillBuffer();
					tuppleId = 0;
				}
				else { 
					break;
				}
			}			
			tp = reader.getTutple(tuppleId++);			
			if (!filter.isSatisfy(tp)) {
				tp = null;
			} 
			else {
				tmp.add(tp);
				if (tmp.size() == bufSize);
			}
		}
		nextBuffer = tmp;
		return nextBuffer.size() > 0;
	}
	
	public static void main(String[] args) throws Exception {
		Catalog cata = new Catalog(new File("data/xmlCatalog.xml"));
		Condition filter = new Condition();
		
		Buffer emp = new Buffer(new File("data/emp.raf"), "Emp", cata, filter);
		emp.open();
		while (emp.hasNext()) {
			List<Tuple> buf = emp.getNext();
			for (Tuple tp : buf) {
				if (tp.getData("Salary") != null) {
					System.out.println(new String(tp.getData("Salary")));
				}
			}
		}
		emp.close();
		
		Buffer dept = new Buffer(new File("data/dept.raf"), "Dept", cata, filter);
		dept.open();
		while (dept.hasNext()) {
			List<Tuple> buf = dept.getNext();
			for (Tuple tp : buf) {
				if (tp.getData("MName") != null) {			
					System.out.println(new String(tp.getData("DName")));
				}
			}
		}
		dept.close();
	}
}
