package iterator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import database.Catalog;
import database.Tuple;

public class SPJ {
	private BufferHelper rHelper, sHelper;
	private Buffer sBuf;
	private List<Buffer> rBuf;
	private String rel1;
	private String rel2;
	private String key;
	private final int rBufSize = 8;

	// private boolean next;
	private int rInx = 0, tupleInR = 0, tupleInS = 0;
	private Tuple next = null;

	public SPJ(String rel1, String rel2, String key, BufferHelper s,
			BufferHelper r) throws Exception {
		this.rel1 = rel1;
		this.rel2 = rel2;
		this.key = key;
		// rHelper = new BufferHelper(new File("data/emp.raf"), "Emp", cata,
		// filter);
		this.sHelper = s;
		this.rHelper = r;
	}

	public void open() throws Exception {

//		sHelper.open();
		if (sHelper.hasNext()) {
			sBuf = sHelper.getNext();
		}
		rBuf = new ArrayList<Buffer>();
		while (rHelper.hasNext() && rBuf.size() < rBufSize) {
			rBuf.add(rHelper.getNext());
		}

		seek();
	}

	public boolean hasNext() {
		return next != null;
	}

	public Tuple getNext() throws Exception {

		Tuple tp = next;
//		next = null;
		seek();
		return tp;
	}

	/**
	 * find next joined Tuple available
	 * 
	 * @throws Exception
	 */
	private void seek() throws Exception {

		while (tupleInS < sBuf.getSize() || sHelper.hasNext()) {
			if (tupleInS < sBuf.getSize()) {
				Tuple sTuple = sBuf.getTuple(tupleInS);
//				if (sTuple.getData("DName") != null) {
//					System.out.println("DName in sTuple =" +new String(sTuple.getData("DName")));
//				}
				// r has several buffer
				while (rInx < rBuf.size() || rHelper.hasNext()) {
					if(rInx < rBuf.size()) {
						// find tuple in current buffer
						while (tupleInR < rBuf.get(rInx).getSize()) {
							Tuple rTuple = rBuf.get(rInx).getTuple(tupleInR);
//							if (rTuple.getData("DName") != null) {
//								System.out.println("DName in rTuple =" +new String(rTuple.getData("DName")));
//							}
							if (sTuple.join(key, rTuple)) {
								next = new Tuple(sTuple, rTuple, key);
								tupleInR++;
//								rHelper.rewind();
//								reloadR(rBuf);
								return;
							}
							// get next tuple
							tupleInR++;
						}
						rInx++;
						// first tuple in next buffer
						tupleInR = 0;
					} else {
						reloadR(rBuf);
						rInx = 0;
						tupleInR = 0;
					}
				}
				tupleInS++;
			} else {
				reload(sBuf, sHelper);
				tupleInS = 0;
			}
			rHelper.rewind();
			reloadR(rBuf);
		}
		//can not find any joinable tuple
		next = null;
	}

	/**
	 * only works for r buffer
	 * @param list
	 * @throws Exception 
	 */
	private void reloadR(List<Buffer> list) throws Exception {
		list.clear();
		while(rHelper.hasNext() && list.size() < rBufSize) {
			list.add(rHelper.getNext());
		}
	}

	private void reload(Buffer buf, BufferHelper helper) throws Exception {
		if(helper.hasNext()) {
			buf = helper.getNext();
		} else {
			buf = null;
		}
	}

	public void close() {
		// make sure when open everything will be the same
		rHelper.close();
		sHelper.close();
		sBuf = null;
		rBuf = null;
		rInx = 0;
		tupleInR = 0;
		tupleInS = 0;
		next = null;
	}
	
	public static void main(String[] args) throws Exception {
		Catalog cata = new Catalog(new File("data/xmlCatalog.xml"));
		Condition filter = new Condition();
		
		BufferHelper emp = new BufferHelper(new File("data/emp.raf"), "Emp", cata, filter);
		emp.open();
//		while (emp.hasNext()) {
//			List<Tuple> buf = emp.getNext().getTuples();			
//			for (Tuple tp : buf) {
//				if (tp.getData("DName") != null) {
//					System.out.println("DName =" +new String(tp.getData("DName")));
//				}
//			}
//		}
//		emp.close();
		
		BufferHelper dept = new BufferHelper(new File("data/dept.raf"), "Dept", cata, filter);
		dept.open();
//		while (dept.hasNext()) {
//			List<Tuple> buf = dept.getNext().getTuples();
//			for (Tuple tp : buf) {
//				if (tp.getData("DName") != null) {
//					System.out.println("DName in dept  =" +new String(tp.getData("DName")));
//				}
//			}
//		}
//		dept.close();
		
		//for SPJ iterator
		
		
		SPJ it = new SPJ("Emp", "Dept", "DName", dept,  emp);
		it.open();
		while(it.hasNext()){
			String str = new String(it.getNext().getData("DName"));
			System.out.println(str);
		}
		it.close();
		
	}

}
