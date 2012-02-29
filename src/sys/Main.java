package sys;

import java.util.logging.Level;

import sys.dht.DHT_Node;

import static sys.utils.Log.*;

/**
 * 
 * @author smd
 * 
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Log.setLevel(Level.ALL);

        sys.Sys.init();
        
        DHT_Node.start();
    }
}
