  package spj;

import java.io.File;
import java.util.List;
  
public class Iterator {
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
	private int tupleLength;
	private int pageNum1, pageNum2;
	private int tupleInBuf1, tupleInBuf2;
	
	private boolean next = false;
	private int bufferNum;
	private List<Boolean> list;
	private int curBuffer;
	private int curTupleIndex1 = 0;
	private int curTupleIndex2 = 0;
	private Tuple curTuple;
	private Sel sel = null;
	private DbRel rel;
	
	private Catalog catalog;
	private int bufferSize = 4096;
	private int pageNumber1 = 0;
	private int pageNumber2 = 0;
	
	
	public Iterator() throws Exception {
		super();
		catalog = new Catalog(new File("/SPJIterator/data/xmlCatalog.xml"));
	}
	public Iterator(Sel selection) throws Exception {
		sel = selection;
		catalog = new Catalog(new File("/SPJIterator/data/xmlCatalog.xml"));
	}
	
	public void open(DbRel rel, int bufferNum) throws Exception {
		this.bufferNum = bufferNum;
		this.rel = rel;
		pageNum1 = (int) Math.ceil(file1.length()/(bufferSize/tupleLength));
		pageNum2 = (int) Math.ceil(file2.length()/(bufferSize/tupleLength));
		//TODO how to set number of buffer needed
		Buffer bufA = new Buffer(file1, bufferSize, pageNumber1, catalog.getDbAttrs(rel.getRelName()));
		pageNumber1++;
		Buffer bufB = new Buffer(file2, bufferSize, pageNumber2, catalog.getDbAttrs(rel.getRelName()));
		pageNumber2++;
		
		for (int i = 0; i < bufA.getTupleTotal(); i++) {
			Tuple A = bufA.getTutple(i);
			Tuple B = bufB.getTutple(curTupleIndex2);
			//update current tuple index in bufA if match
			if (sel != null && (!sel.satisfiedBy(A) || !sel.satisfiedBy(B))) {
				continue;
			} 
			//check relation requirement
			{
				
			}
			
		}
		
		
		
	}
	
	public boolean hasNext() {
		return next;
	}
	
	public String getNext() {
//		while (true) {
//			IF (curTupleIndex is past the last tuple in the last page of Rel) //There are no more tuples in Rel
//			hasNext := False and RETURN ;
//			ELSE { // There are more tuples in Rel
//			IF (curTupleIndex is past the last tuple in the last buffer of Rel in the buffer pool) {
//			reloadBuffers() and RETURN;
//			}
//			ELSE IF (curTupleIndex is past the last tuple in the current buffer in buffer pool){
//			increment curBuffer to the next buffer of Rel in buffer pool;
//			curTupleIndex := the first tuple in the current buffer;
//			}
//			curTuple := the current tuple with index curTupleIndex;
//			increment curTupleIndex to the next tuple in the current buffer;
//			IF (curTuple satisfies the ˇ°selectionˇ± condition)
//			RETURN curTuple;
//			// otherwise, repeat the while loop until we get the next satisfied tuple
//			}
//			}
		return null;
	}
	
	public void reloadBuffers() {
//		decrement the pincount for buffers in the list L;
//				load the next B pages of the relation Rel into the buffer pool incrementing their pin counts;
//				// What if Rel has no pages, less than B pages, or exectly B pages left?
//				curBuffer := the first buffer of the relation Rel in the buffer manager;
//				curTupleIndex := the first tuple in the current buffer;
	}
	
	public void reset() {
//		curBuffer := the first buffer of the relation Rel in the buffer pool;
//		curTupleIndex := the first tuple in the current buffer;
		}
	
	public void close() {
		//deallocate
	}
}
