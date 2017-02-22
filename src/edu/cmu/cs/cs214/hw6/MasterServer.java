package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.StaffUtils;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * This class represents the "master server" in the distributed map/reduce
 * framework. The {@link MasterServer} is in charge of managing the entire
 * map/reduce computation from beginning to end. The {@link MasterServer}
 * listens for incoming client connections on a distinct host/port address, and
 * is passed an array of {@link WorkerInfo} objects when it is first initialized
 * that provides it with necessary information about each of the available
 * workers in the system (i.e. each worker's name, host address, port number,
 * and the set of {@link Partition}s it stores). A single map/reduce computation
 * managed by the {@link MasterServer} will typically behave as follows:
 *
 * <ol>
 * <li>Wait for the client to submit a map/reduce task.</li>
 * <li>Distribute the {@link MapTask} across a set of "map-workers" and wait for
 * all map-workers to complete.</li>
 * <li>Distribute the {@link ReduceTask} across a set of "reduce-workers" and
 * wait for all reduce-workers to complete.</li>
 * <li>Write the locations of the final results files back to the client.</li>
 * </ol>
 */
public class MasterServer extends Thread {
	private static final String TAG = "Master";
	private static final int MAX_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	
    private final int mPort;
    private final List<WorkerInfo> mWorkers;
    private final ExecutorService mExecutor;
    private Map<Integer,WorkerInfo> mShuffleDests;
    private Socket mClientSocket;
    /**
     * The {@link MasterServer} constructor.
     *
     * @param masterPort The port to listen on.
     * @param workers Information about each of the available workers in the
     *        system.
     */
    public MasterServer(int masterPort, List<WorkerInfo> workers) {
    	int numThreads = Math.min(MAX_POOL_SIZE, workers.size());
    	mExecutor = Executors.newFixedThreadPool(numThreads);
        mPort = masterPort;
        mWorkers = workers;
        mClientSocket = null;
        /* determine and store which hash values will be sent to which worker*/
        mShuffleDests = new HashMap<Integer, WorkerInfo>();
    	for(int i = 0; i < mWorkers.size(); i++){
    		mShuffleDests.put(i, mWorkers.get(i));
    	}
    }

    @Override
    public void run() {
    	ServerSocket serverSocket = null;
    	try {
			serverSocket = new ServerSocket(mPort);
		} catch (IOException e1) {
			Log.e(TAG, "Could not open server socket on port " + mPort + ".", e1);
            return;
		}
    	Log.i(TAG, "Listening for incoming commands on port " + mPort + ".");
    	
    	while(true){
    		try {
    			/* read in map and reduce tasks sent by client */
    			mClientSocket = serverSocket.accept();
				ObjectInputStream in = new ObjectInputStream(mClientSocket.getInputStream());
				MapTask mapTask = (MapTask) in.readObject();
		    	ReduceTask reduceTask = (ReduceTask) in.readObject();
		    	
		    	runMapReduce(mapTask, reduceTask);  	
    		} catch (IOException | ClassNotFoundException e) {
				Log.e(TAG, "Error while listening for incoming connections.", e);
                break;
			}
    	}
    	try {
			serverSocket.close();
		} catch (IOException e) {
			Log.e(TAG, "I/O while closing connection.", e);
		}finally{
    		mExecutor.shutdown();
    	}
    }
    /**
     * Method to group the mapreduce functionality. This will make it easier
     * to restart the task if a server fails.
     * @param mt map task
     * @param rt reduce task
     */
    private void runMapReduce(MapTask mt, ReduceTask rt){
    	Set<Partition> takenPartitions = new HashSet<Partition>();
    	List<MapCallable> mapCallables = new ArrayList<MapCallable>();
    	List<ReduceCallable> reduceCallables = new ArrayList<ReduceCallable>();
    	try {
    		/* MAP */
    		/* divide map tasks among workers. this just gives the partition
    		 * to the first worker that has access to that partition */
    		for(WorkerInfo wi : mWorkers){
    			List<Partition> workerPartitions = new ArrayList<Partition>();
    			for(Partition p : wi.getPartitions()){
    				if(!takenPartitions.contains(p)){
    					workerPartitions.add(p);
    					takenPartitions.add(p);	
    				}
    			}
    			if(workerPartitions.size() > 0){
    				mapCallables.add(new MapCallable(mt, wi, workerPartitions));
    			}
    		}
    		List<Future<String>> mapResults = null;
    		try {
    			mapResults = mExecutor.invokeAll(mapCallables);  
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
    		/* SHUFFLE */
    		Set<WorkerInfo> reduceWorkers = new HashSet<WorkerInfo>();
    		try {
    			String resultsFileName = 
    					"shuffleResults" + System.currentTimeMillis() + ".txt";
    			/* give each map worker a shuffle task on its map contents.
    			 * submit these tasks individually so we don't run into
    			 * problems when writing out to the same file */
    			for(int i = 0; i < mapResults.size(); i++){
    				Future<String> fs = mapResults.get(i);
    				/* the parts below that call runMapReduce are to make this
    				 * robust. If for some reason a task got cancelled,
    				 * restart it */
    				if(fs.isCancelled()){
    					runMapReduce(mt,rt);
    				}
    				try{
    					String s = fs.get();
    					WorkerInfo w = mapCallables.get(i).getWorkerInfo();
        				Callable<Set<WorkerInfo>> c = new ShuffleCallable(
        						w, s,mShuffleDests, resultsFileName);
        				Future<Set<WorkerInfo>> fr = mExecutor.submit(c);
        				if(fr.isCancelled()){
        					runMapReduce(mt,rt);
        				}
        				/* build the set of reduce workers - these are the
        				 * workers that received something from the shuffle */
        				Set<WorkerInfo> r = fr.get();
        				reduceWorkers.addAll(r);	
    				} catch (ExecutionException e){
    					/* execution of task threw exception so restart it */
    					runMapReduce(mt,rt);			
    				}	
    			}
    			/* REDUCE */
    			for(WorkerInfo info : reduceWorkers){
    				reduceCallables.add(
    						new ReduceCallable(rt, info, resultsFileName));
    			}
    			List<Future<String>> finalResultsFiles = 
    					mExecutor.invokeAll(reduceCallables);
    			String finalResultsPaths = "";
    			/* create comma-separated string of all final results files */
    			for(int i = 0; i < finalResultsFiles.size(); i++){
    				Future<String> fstr = finalResultsFiles.get(i);
    				if(fstr.isCancelled()){
    					runMapReduce(mt,rt);
    				}
    				String file = fstr.get();
    				finalResultsPaths += (file += ",");
    			}
    			/* send back to the client the final result paths */
    			ObjectOutputStream out = 
    					new ObjectOutputStream(mClientSocket.getOutputStream());
    			out.writeObject(finalResultsPaths);
			} catch (InterruptedException e) {
				Log.e(TAG, "Error while executing tasks.", e);
			} catch (ExecutionException e) {
				/* start computation again if there's failure in computation */
				runMapReduce(mt,rt); 
			} catch (IOException e) {
				Log.e(TAG, "I/O Exception in master server.", e);
			}
    	} finally{
    		/* don't need to do anything here, just waiting for another task */
    	}
    }
 
    /**
     * Class to create callable for map tasks
     * @author Kathleen
     *
     */
    private static class MapCallable implements Callable<String>{
		private final MapTask mTask;
    	private final WorkerInfo workerInfo;
    	private final List<Partition> partitions;
    	private List<String> files; 
    	private String dataPath;
    	/**
    	 * Constructor to instantiate map task callable 
    	 * @param mp Map task
    	 * @param wi Worker info of worker performing task
    	 * @param p Partitions worker needs to map
    	 */
    	public MapCallable(MapTask mp, WorkerInfo wi, List<Partition> p){
    		mTask = mp;
    		workerInfo = wi;
    		partitions = p;
    		dataPath = WorkerStorage.getDataDirectory(workerInfo.getName());
    		files = getFiles();
    	}
    	/**
    	 * Method to get worker info on worker executing this map task
    	 * @return Workerinfo of worker
    	 */
    	public WorkerInfo getWorkerInfo(){
    		return workerInfo;
    	}
    	/**
    	 * Method to get all files associated with the partitions
    	 * @return List of all files this worker needs to map 
    	 */
    	private List<String> getFiles(){
    		List<String> fileList = new ArrayList<String>();
    		for(Partition p : partitions){
    			for(File f : p){
    				String s = dataPath + "\\" + p.getPartitionName() + "\\" + f.getName();
    				fileList.add(s);
    			}
    		}
    		return fileList;
    	}
		@Override
		public String call() throws Exception {
			String workerName = workerInfo.getName();
			String filename = WorkerStorage.getIntermediateResultsDirectory(workerName);
			filename += "\\mapResults" + System.currentTimeMillis() + ".txt";
			Socket socket = null;
			try{
				socket = new Socket(workerInfo.getHost(), workerInfo.getPort());
				
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new ExecuteMapTaskCommand(mTask, filename, files));

				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			
				return (String) in.readObject();
				
			} catch (Exception e){
				Log.e(TAG, "Warning! Received exception while interacting with worker.", e);
                throw e;
			} finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    // Ignore because we're about to exit anyway.
                }
            }
		}
    }
    /**
     * Class to create callable for shuffle tasks
     * @author Kathleen
     *
     */
    private static class ShuffleCallable implements Callable<Set<WorkerInfo>>{
    	private final String mFileName;
    	private final WorkerInfo workerInfo;
    	private final Map<Integer, WorkerInfo> mShuffleDests;
    	private final String mResultsFileName;
    	/**
    	 * Constructor to instantiate object for shuffle tasks.
    	 * @param wi Worker info of worker performing task.
    	 * @param fileName path to file with intermediate map results stored
    	 * @param sd mapping of hash value to corresponding worker
    	 * @param resultsFilename path to file where shuffle results will
    	 * be stored
    	 */
    	public ShuffleCallable(WorkerInfo wi, String fileName, Map<Integer,WorkerInfo> sd,
    			String resultsFileName){
    		mFileName = fileName;
    		workerInfo = wi;
    		mShuffleDests = sd;	
    		mResultsFileName = resultsFileName;
    	}
		@Override
		public Set<WorkerInfo> call() throws Exception {
			Socket socket = null;
			try{
				Set<WorkerInfo> resultWorkers = new HashSet<WorkerInfo>();
				socket = new Socket(workerInfo.getHost(),workerInfo.getPort());	
				ObjectOutputStream out = 
						new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new ExecuteShuffleTaskCommand(mShuffleDests, mFileName, 
						mResultsFileName));
				ObjectInputStream in = 
						new ObjectInputStream(socket.getInputStream());
				String hashedVals = (String) in.readObject();
	
				String[] arr = hashedVals.split(" ");
				/* parse the string of numbers that represent the workers that
				 * received shuffle results, and get the correct worker
				 */
				for(int i = 0; i < arr.length; i++){
					if(arr[i].length() > 0){
						int val = Integer.parseInt(arr[i]);
						WorkerInfo w = mShuffleDests.get(val);
						resultWorkers.add(w);
					}
				}
				return resultWorkers; /* workers that need to do reduce */
			} catch(Exception e){
				Log.e(TAG, "Warning! Received exception while interacting with worker"
						+ " in shuffle callable.", e);
				throw e;
			}
			finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    // Ignore because we're about to exit anyway.
                }
            }
		}	
    }
    /**
     * Class to create callable for reduce task
     * @author Kathleen
     *
     */
    private static class ReduceCallable implements Callable<String>{
    	private final WorkerInfo workerInfo;
    	private final String mResultsFileName;
    	private final ReduceTask mTask;
    	/**
    	 * Constructor to instantiate reduce callable object
    	 * @param rt reduce task
    	 * @param wi worker performing task
    	 * @param resultsFileName file name where final results will be written
    	 */
    	public ReduceCallable(ReduceTask rt, WorkerInfo wi, 
    			String resultsFileName){
    		workerInfo = wi;
    		mResultsFileName = resultsFileName;
    		mTask = rt;
    	}
		@Override
		public String call() throws Exception {
			String workerName = workerInfo.getName();
			/* create file path to where final results will be stored based
			 * on worker */
			String finalFileName = 
					WorkerStorage.getFinalResultsDirectory(workerName);
			finalFileName += 
					"\\finalResults" + System.currentTimeMillis() + ".txt";
			String resultsFilePath = 
					WorkerStorage.getIntermediateResultsDirectory(workerName)
					+ "\\" + mResultsFileName;
			Socket socket = null;
			try{
				/* create command and send it out to the correct port */
				socket = new Socket(workerInfo.getHost(),workerInfo.getPort());
				
				ObjectOutputStream out = 
						new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new ExecuteReduceTaskCommand(
						mTask,resultsFilePath,finalFileName));
				/* read back in final result file string */
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				String resultFile = (String) in.readObject();

				return resultFile;
			} catch(Exception e){
				Log.e(TAG, "Warning! Received exception while interacting with worker"
						+ " in shuffle callable.", e);
				throw e;
			}
			finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    // Ignore because we're about to exit anyway.
                }
            }
		}	
	} 	
    /********************************************************************/
    /***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
    /********************************************************************/

    /**
     * Starts the master server on a distinct port. Information about each
     * available worker in the distributed system is parsed and passed as an
     * argument to the {@link MasterServer} constructor. This information can be
     * either specified via command line arguments or via system properties
     * specified in the <code>master.properties</code> and
     * <code>workers.properties</code> file (if no command line arguments are
     * specified).
     */
    public static void main(String[] args) {
        StaffUtils.makeMasterServer(args).start();
    }
}