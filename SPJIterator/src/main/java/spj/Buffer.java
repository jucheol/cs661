package spj;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

public class Buffer {
	private byte[] buffer;	
	private int numTuples, bufferSize, tupleLength;
	private List<DbAttr> dbAttr;

	public Buffer(File db, int numTuples, int pageNumber, List<DbAttr> dbAttr) throws Exception {
		tupleLength = 0;
		for (DbAttr attr : dbAttr) {
			tupleLength += attr.getAttrLength();
		}
		this.numTuples = numTuples;
		bufferSize = numTuples * tupleLength;
		buffer = new byte[bufferSize];
		RandomAccessFile randomAccess = new RandomAccessFile(db, "r");
		randomAccess.seek(pageNumber * bufferSize);
		randomAccess.read(buffer);
		randomAccess.close();
		this.dbAttr = dbAttr;
	}
	
	public int getNumTuples() {
		return numTuples;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public int getTutpleLength() {
		return tupleLength;
	}

	public Tuple getTutple(int tuppleNumber) {
		if (tuppleNumber < numTuples) {
			int from = tuppleNumber * tupleLength;
			int to = from + tupleLength;
			return new Tuple(dbAttr, Arrays.copyOfRange(buffer, from, to));
		}
		else {
			return null;
		}
	}
}
