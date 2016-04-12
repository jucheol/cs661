package iterator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import database.Reader;
import database.Catalog;
import database.Tuple;

/**
 * Buffer class.
 */
public class BufferHelper {
	private Condition filter;
	private Buffer nextBuffer;
	private Reader reader;
	private int tuppleId, bufSize = 10;
	
	/**
	 * Creates a buffer.
	 *
	 * @param  db database file
	 * @param  relName relation name
	 * @param  cata Catalog object
	 * @param  filter Condition object
	 * @throws Exception IO exception
	 */
	public BufferHelper(File db, String relName, Catalog cata, Condition filter) throws Exception {
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
	
	public Buffer getNext() {		
		return nextBuffer;
	}
	
	public boolean hasNext() throws Exception {
		List<Tuple> tmp = new ArrayList<Tuple>(bufSize);
		while (tmp.size() < bufSize) {
			if (tuppleId == reader.getNumTuples()) {
				if (reader.hasTuple()) {
					reader.fillBuffer();
					tuppleId = 0;
				}
				else { 
					break;
				}
			}			
			Tuple tp = reader.getTutple(tuppleId++);			
			if (filter.isSatisfy(tp)) {				
				tmp.add(tp);				
			}			
		}
		nextBuffer = new Buffer(tmp);
		return tmp.size() > 0;
	}
	
	/**
	 * TODO 
	 * initiates buffer helper again
	 * Then buffer can be loaded from the beginning of file
	 */
	public void rewind() {
		
	}
	
	public static void main(String[] args) throws Exception {
		Catalog cata = new Catalog(new File("data/xmlCatalog.xml"));
		Condition filter = new Condition();
		
		BufferHelper emp = new BufferHelper(new File("data/emp.raf"), "Emp", cata, filter);
		emp.open();
//		while (emp.hasNext()) {
//			List<Tuple> buf = emp.getNext().getTuples();			
//			for (Tuple tp : buf) {
//				if (tp.getData("Salary") != null) {
//					System.out.println(new String(tp.getData("Salary")));
//				}
//			}
//		}
//		emp.close();
		
		BufferHelper dept = new BufferHelper(new File("data/dept.raf"), "Dept", cata, filter);
		dept.open();
//		while (dept.hasNext()) {
//			List<Tuple> buf = dept.getNext().getTuples();
//			for (Tuple tp : buf) {
//				if (tp.getData("MName") != null) {			
//					System.out.println(new String(tp.getData("DName")));
//				}
//			}
//		}
//		dept.close();
//		
//		//for SPJ iterator
//		emp.open();
//		dept.open();
		
		
		SPJ it = new SPJ("Emp", "Dept", "DName", emp, dept);
		it.open();
		while(it.hasNext()){
			System.out.println(it.getNext().toString());
		}
		it.close();
		
	}
}
