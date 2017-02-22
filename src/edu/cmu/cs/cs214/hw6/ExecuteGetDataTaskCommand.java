package edu.cmu.cs.cs214.hw6;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Worker command executor that makes a worker write out to a file the
 * results of a shuffle that were hashed to it.
 * @author Kathleen
 *
 */
public class ExecuteGetDataTaskCommand extends WorkerCommand {
	private static final String TAG = "ExecuteGetDataTaskCommand";
	private static final long serialVersionUID = 8429216608675876146L;
	private List<String> mValues;
	private final String mFileName;
	/**
	 * Constructor to creat worker command for getting data from shuffle
	 * result.
	 * @param fileName filename where shuffle results will be written.
	 * @param values list of values from the shuffle to be written.
	 */
	public ExecuteGetDataTaskCommand(String fileName, List<String> values){
		mValues = values;
		mFileName = fileName;
	}
	@Override
	public void run() {
		try {
			/* write out all values to the shuffleResults file */
			FileOutputStream out = new FileOutputStream(mFileName, true);
			for(String s : mValues){
				s += "\r\n";
				byte[] byteData = s.getBytes();
				out.write(byteData);
			}
			out.flush();
			out.close();

		} catch (FileNotFoundException e) {
			Log.e(TAG, "FileNotFound error while executing get "
					+ "shuffled data task.", e);
		} catch (IOException e) {
			Log.e(TAG, "I/O error while executing get shuffled data task.", e);
		}
	}
}
