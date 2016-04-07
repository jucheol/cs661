  package iterator;

import java.io.File;
import java.util.List;

import database.Buffer;
import database.Catalog;
import database.DbAttr;
import database.Tuple;
  
public class SPJ {
	private SP rIt, sIt;
	private Tuple rBuf;
	private List<Tuple> sBuf;
	
//	Parameters:
//		Rel : the relation to be instreamed;
//		//For example, the path expression //dbRel [@relName="Emp"]
//		B : the number of buffers used for instreaming Rel;
//		// B pages are read, except possibly at the end of the relation stream
//		// Pinning and unpinning must be done - entitled to no more than B pins
//		L: A list to keep track of buffers that have been pinned.
//		Sel: selection condition; // A path expression in the expression tree
//		curTupleIndex : the pointer for the current tuple in the current buffer;
//		curBuffer : the pointer to the current buffer in the buffer manager;
//		curTuple : a small buffer that can hold the data of one tuple temporarily;
//	private String key;
	private File file1 = new File("/SPJIterator/data/dept.raf");
	private File file2 = new File("/SPJIterator/data/emp.raf");
	private SP sPiterator;
	private Catalog catalog;
	private String rel1, rel2; // name of relation to fetch attribute list
	private DbAttr key;
	private Buffer buf;

	private int tupleLength1, tupleLength2;
	private int totalTuple1, totalTuple2;
	private int totalPage1, totalPage2;
	private int numTuples1, numTuples2;
	private int pageVolume = 10;
	
	private int pageNum1 = 0, pageNum2 = 0;
	
	private int tupleInBuf1 = 0, tupleInBuf2 = 0;
	
	private boolean next = false;
	
	
//	private int bufferSize = 4096;

	
	
	public SPJ(String rel1, String rel2, DbAttr key, SP it) throws Exception {
		this.rel1 = rel1;
		this.rel2 = rel2;
		this.key = key;
		this.sPiterator = it;
		catalog = new Catalog(new File("/SPJIterator/data/xmlCatalog.xml"));
		tupleLength1 = catalog.getTupleLength(rel1);
		tupleLength2 = catalog.getTupleLength(rel2);
		
		totalTuple1 = (int) file1.length()/tupleLength1;
		totalTuple2 = (int) file1.length()/tupleLength2;
		
		totalPage1 = (int) Math.ceil(totalTuple1 / pageVolume);
		totalPage2 = (int) Math.ceil(totalTuple2 / pageVolume);
	}
	
	public void open() throws Exception {
		
		
		
		reloadBuffers();
		
	/*	numTuples2 = pageVolume;
		if (totalTuple2 < pageVolume) numTuples2 = totalTuple2;
		Buffer bufB = new Buffer(file2, numTuples2, pageNum2, catalog.getDbAttrs(rel2));
		pageNum2++;*/
		
		/*while(sPiterator.hasNext()&& pageNum1 < totalPage1){
			for (int i = 0; i < numTuples1; i++) {
				if (buf.getTutple(i).join(key, sPiterator.getNext())){
					tupleInBuf1 = i;
					next = true;
					return;
				}
			}
		}*/
		//find first tuple
		seek();
	}
	
	public boolean hasNext() {
		return next;
	}
	
	public Tuple getNext() {

		Tuple tp = buf.getTutple(tupleInBuf1);
		seek();
		return tp;
	}
	
	private void reloadBuffers() throws Exception {
				numTuples1 = pageVolume;
				if (totalTuple1 < pageVolume) numTuples1 = totalTuple1;
				buf = new Buffer(file1, numTuples1, pageNum1, catalog.getDbAttrs(rel1));
				pageNum1++;
	}
	
	private void seek(){
		while(sPiterator.hasNext()&& pageNum1 < totalPage1){
			for (int i = 0; i < numTuples1; i++) {
				if (buf.getTutple(i).join(key, sPiterator.getNext())){
					tupleInBuf1 = i;
					next = true;
					return;
				}
			}
		}
		next = false;
	}
	
	
	public void close() {
		//deallocate
		pageNum1 = 0;
		tupleInBuf1 = 0;
		buf = null;
	}
}
