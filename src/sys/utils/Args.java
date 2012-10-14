package sys.utils;

/**
 * 
 * Convenience class for parsing main class arguments...
 * 
 * @author smduarte
 *
 */
public class Args {

    //Can this be generalized????
    
    static public String valueOf( String[] args, String flag, String defaultValue ) {
        for( int i = 0; i < args.length - 1; i++ )
            if( flag.equals( args[i]) )
                return args[i+1] ;
        return defaultValue;
    }
    
    static public int valueOf( String[] args, String flag, int defaultValue ) {
        for( int i = 0; i < args.length - 1; i++ )
            if( flag.equals( args[i]) )
                return Integer.parseInt( args[i+1] );
        return defaultValue;
    }

    static public double valueOf( String[] args, String flag, double defaultValue ) {
        for( int i = 0; i < args.length - 1; i++ )
            if( flag.equals( args[i]) )
                return Double.parseDouble( args[i+1] );
        return defaultValue;
    }

    static public boolean valueOf( String[] args, String flag, boolean defaultValue ) {
        for( int i = 0; i < args.length - 1; i++ )
            if( flag.equals( args[i]) )
                return Boolean.parseBoolean( args[i+1] );        
        return defaultValue;
    }

}
