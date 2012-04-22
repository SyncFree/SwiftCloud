package sys.dht.catadupa;

import static sys.Sys.*;

public class Catadupa {
    public static enum Scope { DATACENTER, UNIVERSAL } ;
    
    static String DOMAIN = "Catadupa";
    static Scope SCOPE = Scope.UNIVERSAL;
    
    private Catadupa() {        
    }
    
    public static void setScope( Scope scope ) {
        SCOPE = scope;
    }
    
    public static void setDomain(String domain) {
        DOMAIN = domain;
    }
    
    public static void setScopeAndDomain( Scope scope, String domain ) {
        SCOPE = scope;
        DOMAIN = domain;
    }
    
    static String discoveryName() {
        if( SCOPE == Scope.UNIVERSAL )
            return DOMAIN;
        else
            return DOMAIN + "-" + Sys.getDatacenter();
    }
}
