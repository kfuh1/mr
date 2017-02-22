package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
/**
 * Class to implement Emitter interface for writing intermediate and final
 * results to a file.
 * @author Kathleen
 *
 */
public class FileEmitter implements Emitter, Serializable {

	private static final long serialVersionUID = 1283753349661679843L;
	private final String mFileName;
	private final File mFile;
	private transient FileOutputStream mOut;
	private boolean isOpen;
	/**
	 * Constructor for FileEmitter that initializes the filename and
	 * outputstream
	 * @param fileName name of file to write to
	 */
	public FileEmitter(String fileName){
		mFileName = fileName;
		mFile = new File(mFileName);
		isOpen = false; 
	}
	@Override
	public void close() throws IOException {
		mOut.flush();
		mOut.close();
	}

	@Override
	public synchronized void emit(String key, String value) throws IOException {
		/* create new outputstream if one doesn't exist.
		 * This outputstream will be left open until all writes for the task
		 * are complete */
		if(!isOpen){
			mOut = new FileOutputStream(mFile, true);
			isOpen = true;
		}
		String data = key + "," + value + "\r\n";
		byte[] dataBytes = data.getBytes();
		mOut.write(dataBytes);
	}
}