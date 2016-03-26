package spj;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

public class Buffer {
	private byte[] buffer;	
	private int maxTuple, tutpleLength;
	private List<DbAttr> dbAttr;

	public Buffer(File db, int bufferSize, int pageNumber, List<DbAttr> dbAttr) throws Exception {
		buffer = new byte[bufferSize];
		RandomAccessFile randomAccess = new RandomAccessFile(db, "r");
		randomAccess.seek(pageNumber * bufferSize);
		randomAccess.read(buffer);
		randomAccess.close();
		int tutpleLength = 0; 
		for (DbAttr attr : dbAttr) {
			tutpleLength += attr.getAttrLength();
		}
		maxTuple = bufferSize / tutpleLength;
		this.dbAttr = dbAttr;
	}

	public Tuple getTutple(int tuppleNumber) {
		if (tuppleNumber < maxTuple) {
			int from = tuppleNumber * tutpleLength;
			int to = from + tutpleLength;
			return new Tuple(dbAttr, Arrays.copyOfRange(buffer, from, to));
		}
		else {
			return null;
		}
	}
}
