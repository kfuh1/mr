package edu.cmu.cs.cs214.hw6.plugin.wordprefix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.ReduceTask;

/**
 * The reduce task for a word-prefix map/reduce computation.
 */
public class WordPrefixReduceTask implements ReduceTask {
    private static final long serialVersionUID = 6763871961687287020L;

    @Override
    public void execute(String key, Iterator<String> values, Emitter emitter) throws IOException {
    	Map<String, Integer> occurrences = new HashMap<String, Integer>();
    	/* create mapping of words to number of occurrences */
    	while(values.hasNext()){
    		String word = values.next();
    		if(!occurrences.containsKey(word)){
    			occurrences.put(word, 1);
    		}
    		else{
    			int num = occurrences.get(word);
    			occurrences.put(word, num+1);
    		}
    	}
    	/* find the word that occurs the most for given key */
    	int maxOccurNum = 0;
    	String mostFreqWord = "";
    	for(String s : occurrences.keySet()){
    		int num = occurrences.get(s);
    		if(num > maxOccurNum){
    			maxOccurNum = num;
    			mostFreqWord = s;
    		}
    	}
    	emitter.emit(key, mostFreqWord);
    }

}
