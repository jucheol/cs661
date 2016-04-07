package iterator;

import java.io.File;

import database.Reader;
import database.Catalog;
import database.Tuple;

/**
 * SP iterator.
 */
public class SP {
	private Condition filter;
	private Tuple nextTuple;
	private Reader buf;
	private int tuppleId;
	
	/**
	 * Creates a SP iterator.
	 *
	 * @param  db database file
	 * @param  relName relation name
	 * @param  cata Catalog object
	 * @param  filter Condition object
	 * @throws Exception IO exception
	 */
	public SP(File db, String relName, Catalog cata, Condition filter) throws Exception {
		this.filter = filter;		
		buf = new Reader(db, 10, cata.getDbAttrs(relName));
	}
	
	public void open() throws Exception {
		tuppleId = 0;
		buf.fillBuffer();		
	}
	
	public void close() {
		System.err.println("SP iterator closed.");
	}
	
	public Tuple getNext() {		
		return nextTuple;
	}
	
	public boolean hasNext() throws Exception {
		Tuple tp = null;
		while (tp == null) {
			if (tuppleId == buf.getNumTuples()) {
				if (buf.hasTuple()) {
					buf.fillBuffer();
					tuppleId = 0;
				}
				else { 
					break;
				}
			}			
			tp = buf.getTutple(tuppleId++);
			if (!filter.isSatisfy(tp)) {
				tp = null;
			}
		}
		nextTuple = tp;
		return nextTuple != null;
	}
	
	public static void main(String[] args) throws Exception {
		Catalog cata = new Catalog(new File("data/xmlCatalog.xml"));
		Condition filter = new Condition();
		
		SP emp = new SP(new File("data/emp.raf"), "Emp", cata, filter);
		emp.open();
		while (emp.hasNext()) {
			Tuple tp = emp.getNext();
			if (tp.getData("Salary") != null) {
				System.out.println(new String(tp.getData("Salary")));
			}
		}
		emp.close();
		
		SP dept = new SP(new File("data/dept.raf"), "Dept", cata, filter);
		dept.open();
		while (dept.hasNext()) {
			Tuple tp = dept.getNext();	
			if (tp.getData("MName") != null) {			
				System.out.println(new String(tp.getData("DName")));
			}
		}
		dept.close();
	}
}
