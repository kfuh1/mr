package edu.cmu.cs.cs214.hw6;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.util.Log;
/**
 * Class to perform reduce commands.
 * @author Kathleen
 *
 */
public class ExecuteReduceTaskCommand extends WorkerCommand{
	private static final String TAG = "ExecuteReduceTaskCommand";
	private static final long serialVersionUID = -4307869259447917190L;
	private final ReduceTask mTask;
	private final String mShuffleResultFile;
	private final String mOutFileName;
	private final Emitter mEmitter;
	private final Map<String, List<String>> mKeyAndValues;
	/**
	 * Constructor to create tasks for executing reduce tasks.
	 * @param rt reduce task
	 * @param shuffleResultFile path to file storing data that needs to be
	 * reduced (i.e. the data from the shuffle phase hashed to this worker)
	 * @param outFileName file to write final results to
	 */
	public ExecuteReduceTaskCommand(ReduceTask rt, String shuffleResultFile,
			String outFileName){
		mTask = rt;
		mShuffleResultFile = shuffleResultFile;
		mOutFileName = outFileName;
		mEmitter = new FileEmitter(mOutFileName);
		mKeyAndValues = new HashMap<String, List<String>>();
	}
	@Override
	public void run() {
		Socket socket = getSocket();
		List<String> valList = null;
		FileInputStream in = null;
		try {
			synchronized(this){
				/* for each unique key put into hashmap that maps a key to
				 * a list of its values from the file */
				in = new FileInputStream(mShuffleResultFile);
				Scanner scanner = new Scanner(in);
				scanner.useDelimiter("\r\n");
				while(scanner.hasNext()){
					String pair = scanner.next().trim().toLowerCase();
					String[] arr = pair.split(",");
					/* we know we'll have key, value pairs so these array
					 * accesses will be safe. */
					String key = arr[0];
					String val = arr[1];
					if(!mKeyAndValues.containsKey(key)){
						valList = new ArrayList<String>();
					}
					else{
						valList = mKeyAndValues.get(key);
					}
					valList.add(val);
					mKeyAndValues.put(key, valList);
				}
				scanner.close();
			}
			for(String k : mKeyAndValues.keySet()){
				try {
					mTask.execute(k,mKeyAndValues.get(k).iterator(),mEmitter);	
				} catch (IOException e) {
					Log.e(TAG, "I/O error while executing reduce task.", e);
				}
			}	
			try{
				/* send back to master the filepath where the final results
				 * were written */
				ObjectOutputStream out = 
						new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(mOutFileName);
				mEmitter.close();
			} catch(IOException e){
				Log.e(TAG, "I/O error while executing reduce task.", e);
			}	
		} catch (FileNotFoundException e) {
			Log.e(TAG, "File not found error while executing reduce task.", e);
		}	
	}
}
