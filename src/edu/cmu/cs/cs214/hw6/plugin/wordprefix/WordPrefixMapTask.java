package edu.cmu.cs.cs214.hw6.plugin.wordprefix;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.MapTask;

/**
 * The map task for a word-prefix map/reduce computation.
 */
public class WordPrefixMapTask implements MapTask {
    private static final long serialVersionUID = 3046495241158633404L;

    @Override
    public void execute(InputStream in, Emitter emitter) throws IOException {
    	Scanner scanner = new Scanner(in);
    	scanner.useDelimiter("\\W+");
    	while(scanner.hasNext()){
    		String word = scanner.next().trim().toLowerCase();
    		/* for every word go through every prefix it has and emit
    		 * a prefix/word pair */
    		for(int i = 1; i < word.length(); i++){
    			String substr = word.substring(0,i);
    			emitter.emit(substr, word);
    		}
    	}
    	scanner.close();
    }

}
