package database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;

import cyclients.spj.Catalog;
import cyclients.spj.DbAttr;
import cyclients.spj.DbRel;

/**
 * A sample data generator
 */
public class SampleGenerator {
	public static void create(String cataName, String num) throws Exception {		
		Catalog cata = new Catalog(new File(cataName));
		int count = Integer.parseInt(num);
		for (DbRel rel : cata.getDbRels()) {
			File relFile = new File("cyclients/spj/workspace/" + rel.getRelName() + ".bin");
			if (relFile.exists()) {
				relFile.delete();
			}			
			RandomAccessFile relDb = new RandomAccessFile(relFile, "rw");
			int offset = 0;
			for (int id = 0; id < count; id++) {				
				for (DbAttr attr : cata.getDbAttrs(rel.getRelName())) {
					byte[] data = null;
					if (attr.getAttrType().equals("integer")) {
						data = RandomStringUtils.randomNumeric(attr.getAttrLength()).getBytes();
					}
					else {
						data = RandomStringUtils.randomAlphabetic(attr.getAttrLength()).getBytes();
					}							
					relDb.seek(offset);
					relDb.write(data);
					offset += attr.getAttrLength();
				}				
			}
			relDb.close();
		}
	}
	
	public static void main(String[] args) throws Exception {		
		File empF = new File("data/emp.raf");
		if (empF.exists()) {
			empF.delete();
		}
		File deptF = new File("data/dept.raf");
		if (deptF.exists()) {
			deptF.delete();
		}
		RandomAccessFile empDb = new RandomAccessFile(empF, "rw");
		RandomAccessFile deptDb = new RandomAccessFile(deptF, "rw");		
		List<String> DNames = new LinkedList<String>();		
		int empCount = 2000, deptCount = 40;				
		BufferedWriter bw = new BufferedWriter(new FileWriter("data/dept.txt"));
		for (int offSet = 0; offSet < deptCount; offSet++) {			
			String DNameStr = RandomStringUtils.randomAlphabetic(10);
			byte[] DName = DNameStr.getBytes();			
			DNames.add(DNameStr);
			String MNameStr = RandomStringUtils.randomAlphabetic(20);			
			byte[] MName = MNameStr.getBytes();
			byte[] dept = ArrayUtils.addAll(DName, MName);
			deptDb.seek(offSet * 30);
			deptDb.write(dept);
			bw.write("DName=[" + DNameStr + "], MName=[" + MNameStr + "]\n");
		}	
		bw.close();
		bw = new BufferedWriter(new FileWriter("data/emp.txt"));
		for (int offSet = 0; offSet < empCount; offSet++) {
			String NameStr = RandomStringUtils.randomAlphabetic(20);
			byte[] Name = NameStr.getBytes();
			String DNameStr = DNames.get(offSet % deptCount);
			byte[] DName = DNameStr.getBytes();
			String SalaryStr = RandomStringUtils.randomNumeric(4); 
			byte[] Salary = SalaryStr.getBytes();			
			byte[] tName = ArrayUtils.addAll(Name, DName);
			byte[] emp = ArrayUtils.addAll(tName, Salary);
			empDb.seek(offSet * 34);
			empDb.write(emp);
			bw.write("Name=[" + NameStr + "], DName=[" + DNameStr + "], Salary=["+ SalaryStr +"]\n");
		}
		bw.close();		
		empDb.close();
		deptDb.close();
	}
}
