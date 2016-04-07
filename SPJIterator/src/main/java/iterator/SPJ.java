package iterator;


	  import java.io.File;
import java.util.List;

	  import database.Reader;
import database.Catalog;
import database.DbAttr;
import database.Tuple;
	    
	  public class SPJ {
	  	private Buffer sBuf;
	  	private List<Buffer> rBuf;
		private String rel1;
		private String rel2;
		private DbAttr key;
		private boolean next;
	  	
  	
	  	
	  	public SPJ(String rel1, String rel2, DbAttr key, Buffer s, List<Buffer> r) throws Exception {
	  		this.rel1 = rel1;
	  		this.rel2 = rel2;
	  		this.key = key;
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
	  				buf = new Reader(file1, numTuples1, pageNum1, catalog.getDbAttrs(rel1));
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
	  

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
