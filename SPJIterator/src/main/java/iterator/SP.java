package iterator;

import java.io.File;

import database.Buffer;
import database.Catalog;
import database.Tuple;

public class SP {
	private Condition filter;
	private Tuple nextTuple;
	private Buffer buf;
	private int tuppleId;
	
	public SP(File db, String relName, Catalog cata, Condition filter) throws Exception {
		this.filter = filter;		
		buf = new Buffer(db, 10, cata.getDbAttrs(relName));
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
			String output = new String(tp.getData("Salary"));
			System.out.println(output);
		}
		emp.close();
		
		SP dept = new SP(new File("data/dept.raf"), "Dept", cata, filter);
		dept.open();
		while (dept.hasNext()) {
			Tuple tp = dept.getNext();			
			String output = new String(tp.getData("DName"));
			System.out.println(output);
		}
		dept.close();
	}
}
