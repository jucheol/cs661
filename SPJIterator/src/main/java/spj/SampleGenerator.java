package spj;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;

public class SampleGenerator {
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
		List<byte[]> DNames = new LinkedList<byte[]>();		
		int empCount = 2000, deptCount = 40;		
		for (int offSet = 0; offSet < deptCount; offSet++) {			
			byte[] DName = RandomStringUtils.randomAlphabetic(10).getBytes();
			DNames.add(DName);
			byte[] MName = RandomStringUtils.randomAlphabetic(20).getBytes();
			byte[] dept = ArrayUtils.addAll(DName, MName);
			deptDb.seek(offSet * 30);
			deptDb.write(dept);
		}		
		for (int offSet = 0; offSet < empCount; offSet++) {			
			byte[] Name = RandomStringUtils.randomAlphabetic(20).getBytes();
			byte[] DName = DNames.get(offSet % deptCount);
			byte[] Salary = RandomStringUtils.randomNumeric(4).getBytes();			
			byte[] tName = ArrayUtils.addAll(Name, DName);
			byte[] emp = ArrayUtils.addAll(tName, Salary);
			empDb.seek(offSet * 34);
			empDb.write(emp);
		}
		empDb.close();
		deptDb.close();
	}
}
