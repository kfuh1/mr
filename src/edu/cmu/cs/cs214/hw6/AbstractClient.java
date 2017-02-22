package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import edu.cmu.cs.cs214.hw6.plugin.wordcount.WordCountClient;
import edu.cmu.cs.cs214.hw6.plugin.wordprefix.WordPrefixClient;
import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * An abstract client class used primarily for code reuse between the
 * {@link WordCountClient} and {@link WordPrefixClient}.
 */
public abstract class AbstractClient {
	private static final String TAG = "AbstractClient";
    private final String mMasterHost;
    private final int mMasterPort;

    /**
     * The {@link AbstractClient} constructor.
     *
     * @param masterHost The host name of the {@link MasterServer}.
     * @param masterPort The port that the {@link MasterServer} is listening on.
     */
    public AbstractClient(String masterHost, int masterPort) {
        mMasterHost = masterHost;
        mMasterPort = masterPort;
    }

    protected abstract MapTask getMapTask();

    protected abstract ReduceTask getReduceTask();

    public void execute() {
        final MapTask mapTask = getMapTask();
        final ReduceTask reduceTask = getReduceTask();
        
        Socket socket = null;
        try {
        	/* connect to master server and send reduce and map tasks
        	 Note: must send the MapTask first, then the ReduceTask*/
			socket = new Socket(mMasterHost, mMasterPort);
			ObjectOutputStream out = 
					new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(mapTask);
			out.writeObject(reduceTask);
			
			/* get results of mapreduce -
			 * this will be comma-separated list of file paths to the 
			 * final results - the client can do what it wants with the data */
			ObjectInputStream in = 
					new ObjectInputStream(socket.getInputStream());
			Object filePaths = (String) in.readObject();
			
			System.out.println(filePaths);
			
		} catch (UnknownHostException e) {
			Log.e(TAG, "Problems connecting to master server.", e);
		} catch (IOException e) {
			Log.e(TAG, "Problems sending data to master server.", e);
		} catch (ClassNotFoundException e) {
			Log.e(TAG, "Problems getting data from master server.", e);
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