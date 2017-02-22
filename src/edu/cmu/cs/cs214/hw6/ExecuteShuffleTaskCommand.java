package edu.cmu.cs.cs214.hw6;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;
/**
 * Class to perform shuffle tasks.
 * @author Kathleen
 *
 */
public class ExecuteShuffleTaskCommand extends WorkerCommand {
	private static final int MAX_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	private static final long serialVersionUID = -8044612255466622563L;
	private static final String TAG = "ExecuteShuffleTaskCommand";
	private final Map<Integer,WorkerInfo> mShuffleDests;
	private Map<Integer, List<String>> mShuffleResults;
	private final String mFilePath;
	private final String mShuffleResultsFileName;
	private transient ExecutorService mExecutor;

	/**
	 * Constructor to instantiate object for executing shuffle command.
	 * @param sdMap Mapping of hash value to the worker to go to.
	 * @param filePath File where the map values that need shuffling are.
	 * @param shuffleResultsFileName File where shuffle results will be stored
	 */
	public ExecuteShuffleTaskCommand(Map<Integer, WorkerInfo> sdMap, 
			String filePath, String shuffleResultsFileName){
		mShuffleDests = sdMap;
		mFilePath = filePath;
		mShuffleResultsFileName = shuffleResultsFileName;
	}
	@Override
	public void run() {
		List<String> ls = new ArrayList<String>();
		mShuffleResults = new HashMap<Integer, List<String>>();
		FileInputStream in = null;
		try {
			in = new FileInputStream(mFilePath);
			Scanner scanner = new Scanner(in);
			scanner.useDelimiter("\r\n");
			/* for each key/value pair read in, hash the key and store
			 * that pair with the correct worker based on hashcode  */
			while (scanner.hasNext()) {
	            String key = scanner.next().trim();
	            if(key.length() > 0){
	            	String[] arr = key.split(",");
	            	String word = arr[0];
	            	int hashVal = 
	            			Math.abs(word.hashCode() % mShuffleDests.size());
	            	if(mShuffleResults.containsKey(hashVal)){
	            		ls = mShuffleResults.remove(hashVal);
	            	}
	            	else{
	            		ls = new ArrayList<String>();
	            	}
	            	ls.add(key);
            		mShuffleResults.put(hashVal, ls);
	            }
	        }
			scanner.close();
			String serversHashedTo = sendToWorker();
		
			/* send back to master the servers that received shuffle results */
			Socket masterSocket = getSocket();
			ObjectOutputStream out = 
					new ObjectOutputStream(masterSocket.getOutputStream());
			out.writeObject(serversHashedTo);
			
		} catch (FileNotFoundException e) {
			Log.e(TAG, "FileNotFound error while executing shuffle task.", e);
		} catch (IOException e) {
			Log.e(TAG, "I/O error while executing shuffle task.", e);
		}
		
	}
	/**
	 * Method to send shuffle results to workers and have them write them
	 * to their own directories.
	 * @return Comma-separated string of workers that servers that received
	 * shuffle results.
	 */
	private synchronized String sendToWorker(){
		int numThreads = Math.min(MAX_POOL_SIZE, mShuffleDests.size());
    	mExecutor = Executors.newFixedThreadPool(numThreads);
		List<GetDataCallable> callables = new ArrayList<GetDataCallable>();
		String hashedValsStr = "";
		/* for each shuffled value, create a connection with that worker
		 * and create new GetDataCallable task for them to write it out.
		 */
		for(Integer k : mShuffleResults.keySet()){
			WorkerInfo info = mShuffleDests.get(k);
			String name = info.getName();
			int port = info.getPort();
			String host = info.getHost();
			String filename = 
					WorkerStorage.getIntermediateResultsDirectory(name)
					+ "\\" + mShuffleResultsFileName;
			List<String> values = mShuffleResults.get(k);
			callables.add(new GetDataCallable(host, port, filename, values));
			hashedValsStr += (k + " ");	
		}
		try {
			mExecutor.invokeAll(callables);
		} catch (InterruptedException e) {
			Log.e(TAG, "Connection error while executing shuffle task.", e);
		} finally{
			mExecutor.shutdown();
		}
		return hashedValsStr;
	}
	/**
	 * Class to create callable to tell worker to write out the shuffle results
	 * @author Kathleen
	 *
	 */
	private static class GetDataCallable implements Callable<String>{
		private final String mHost;
		private final int mPort;
		private final String mFileName;
		private final List<String> mValues;
		/**
		 * Constructor for creating a callable task of writing shuffled data
		 * @param host Destination worker's host
		 * @param port Destination worker's port
		 * @param filename File where shuffled results will be written to
		 * @param values Shuffled values that need to be written
		 */
		public GetDataCallable(String host, int port, String filename,
				List<String> values){
			mHost = host;
			mPort = port;
			mFileName = filename;
			mValues = values;
		}
		@Override
		public String call() throws Exception {
			try {
				Socket socket = new Socket(mHost, mPort);
				
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				
				out.writeObject(new ExecuteGetDataTaskCommand(mFileName,mValues));
				socket.close();
				
			} catch (UnknownHostException e) {
				Log.e(TAG, "Connection exception while trying to send data to worker.", e);
			} catch (IOException e) {
				Log.e(TAG, "I/O exception while trying to send data to worker.", e);
			}
			return "";
		}	
	}
}
