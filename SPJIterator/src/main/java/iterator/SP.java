package iterator;

import java.io.File;

import database.Catalog;
import database.Tuple;

public class SP {
	private Condition filter;
	private Tuple nextTuple;
	
	public SP(File db, String relName, Catalog cata, Condition filter) {
		this.filter = filter;
		
	}
	
	public void open() {
		
	}
	
	public void close() {
		
	}
	
	public Tuple getNext() {
		Tuple tp = this.getNext();
		while (!filter.isSatisfy(tp)) {
			tp = this.getNext();
		}
		
		return tp;
	}
	
	public boolean hasNext() {
		return nextTuple != null;
	}
}
