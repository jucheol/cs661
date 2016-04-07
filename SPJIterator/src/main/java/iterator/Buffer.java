package iterator;

import java.util.List;

import database.Tuple;

public class Buffer {
	private List<Tuple> tuples;
	
	public Buffer(List<Tuple> buffers) {
		this.tuples = buffers;
	}
	
	public Tuple getTuple(int id) {
		return tuples.get(id);
	}	
	
	public int getSize() {
		return tuples.size();
	}
	
	public List<Tuple> getTuples() {
		return tuples;
	}
}
