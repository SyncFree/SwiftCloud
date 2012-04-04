package sys;

import static sys.utils.Log.Log;

import java.util.logging.Level;

import sys.dht.catadupa.CatadupaNode;

/**
 * 
 * @author smd
 * 
 */
public class Main {

	public static void main(String[] args) throws Exception {
		Log.setLevel(Level.ALL);

		sys.Sys.init();

		new CatadupaNode().init();
	}
}
