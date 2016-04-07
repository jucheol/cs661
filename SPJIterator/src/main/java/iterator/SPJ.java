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
		private String key;
//		private boolean next;
		private int rInx = 0, tupleInR = 0, tupleInS = 0;
		private Tuple next = null;
	  	
  	
	  	
	  	public SPJ(String rel1, String rel2, String key, Buffer s, List<Buffer> r) throws Exception {
	  		this.rel1 = rel1;
	  		this.rel2 = rel2;
	  		this.key = key;
	  		this.sBuf = s;
	  		this.rBuf = r;
	  	}
	  	
	  	public void open() throws Exception {
	  		
	  		//find first tuple
	  		seek();
	  	}
	  	
	  	public boolean hasNext() {
	  		return next != null;
	  	}
	  	
	  	public Tuple getNext() throws Exception {

	  		Tuple tp = next;
	  		seek();
	  		return tp;
	  	}
	  	

	  	/**
	  	 * find next joined Tuple available
	  	 * @throws Exception 
	  	 */
	  	private void seek() throws Exception{
	  		while(sBuf.hasNext()){
	  			Tuple sTuple = sBuf.getNext().get(0);
	  			while(rInx < rBuf.size())   {
	  				while(rBuf.get(rInx).hasNext()){
	  					Tuple rTuple = rBuf.get(rInx).getNext().get(0);
	  					if (sTuple.join(key, rTuple)){
	  						next = new Tuple(sTuple, rTuple, key);
	  						return;
	  					}
	  				}
	  				rInx++;
	  			}
	  			rInx = 0;
	  		}
	  		next = null;
	  	}
	  	
	  	
	  	public void close() {
	  		//deallocate
	  		rInx = 0; 
	  		tupleInR = 0; 
	  		tupleInS = 0;
			next = null;
	  	}
	  



}
