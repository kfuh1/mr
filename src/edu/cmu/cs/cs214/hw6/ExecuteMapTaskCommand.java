package edu.cmu.cs.cs214.hw6;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

import edu.cmu.cs.cs214.hw6.util.Log;
/**
 * Class to perform map task commands. 
 * @author Kathleen
 *
 */
public class ExecuteMapTaskCommand extends WorkerCommand{
	private static final long serialVersionUID = -1407123069761929144L;
	private static final String TAG = "ExecuteMapTaskCommand";
	
	private final MapTask mTask;
	private final String mOutFileName;
	private final List<String> mInFileNames;
	private final Emitter mEmitter;
	/**
	 * Constructor for ExecuteMapTaskCommand to initialize variables
	 * @param task MapTask to be applied
	 * @param outFileName File to write intermediate results to
	 * @param inFileNames Files in the partitions that a worker needs to work on
	 * @throws IOException Problems reading input data or writing mapped data
	 */
	public ExecuteMapTaskCommand(MapTask task, String outFileName, 
			List<String> inFileNames) throws IOException{
		mTask = task;
		mOutFileName = outFileName;
		mInFileNames = inFileNames;
		mEmitter = new FileEmitter(mOutFileName);
	}
	@Override
	public void run(){
		Socket socket = getSocket();
		FileInputStream in = null;
		/* map the data for each file in partitions assigned to this worker */
		for(String f : mInFileNames){
			try {
				synchronized(this){
					in = new FileInputStream(f);
					mTask.execute(in, mEmitter);	
					in.close();
				}
			} catch (FileNotFoundException e) {
				Log.e(TAG, "I/O error while executing map task.", e);
			} catch (IOException e) {
				Log.e(TAG, "I/O error while executing map task.", e);
			}	
		}
		try {
			/* tell master server map task is completed and give back path to
			 * file that was written to. */
			ObjectOutputStream out = 
					new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(mOutFileName);
			mEmitter.close();	
		} catch (IOException e) {
			Log.e(TAG, "I/O error while executing map task.", e);
		}
	}
}