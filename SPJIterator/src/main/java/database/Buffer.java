package database;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

/**
 * Buffer stores an information from a database file.
 */
public class Buffer {
	private byte[] buffer;	
	private int numTuples, bufferSize, tupleLength, pageNumber, remainTuples;
	private List<DbAttr> dbAttr;
	private File db;

	/**
	 * Creates a buffer.
	 *
	 * @param  db database file
	 * @param  numTuples the number of tuples in a buffer
	 * @param  pageNumber the page number for the database file
	 * @param  dbAttr database attributes
	 * @throws Exception IO exception
	 */
	public Buffer(File db, int numTuples, List<DbAttr> dbAttr) {
		tupleLength = 0;
		pageNumber = 0;
		for (DbAttr attr : dbAttr) {
			tupleLength += attr.getAttrLength();
		}
		this.numTuples = numTuples;
		bufferSize = numTuples * tupleLength;
		remainTuples = (int) db.length() / tupleLength;
		this.dbAttr = dbAttr;
		this.db = db;
	}
	
	public void fillBuffer() throws Exception {
		RandomAccessFile randomAccess = new RandomAccessFile(db, "r");
		if (remainTuples < numTuples) {
			numTuples = remainTuples;
			buffer = new byte[remainTuples * tupleLength];			
			randomAccess.seek(pageNumber++ * bufferSize);
			randomAccess.read(buffer);
			remainTuples = 0;
		}
		else {
			buffer = new byte[bufferSize];			
			randomAccess.seek(pageNumber++ * bufferSize);
			randomAccess.read(buffer);
			remainTuples -= numTuples;
		}
		randomAccess.close();
	}
	
	public boolean hasTuple() {
		return remainTuples > 0 ? true : false;
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

	public Tuple getTutple(int tuppleId) {
		if (tuppleId < numTuples) {
			int from = tuppleId * tupleLength;
			int to = from + tupleLength;
			return new Tuple(dbAttr, Arrays.copyOfRange(buffer, from, to));
		}
		else {
			return null;
		}
	}
}