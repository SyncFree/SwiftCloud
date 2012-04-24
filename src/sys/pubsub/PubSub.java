package sys.pubsub;

import sys.net.api.Endpoint;


abstract public class PubSub {

    public interface Handler {
        void notify( String group, Object payload );
    }    

    public abstract void publish( String group, Object payload ); 
    
    public abstract void addRemoteSubscriber( String group, Endpoint subscriber ) ;
    
    public abstract void subscribe( String group, Handler handler );
    
    public abstract void unsubscribe( String group, Handler handler );
    
    
    protected PubSub() {
        PubSub = this;
    }

    public static PubSub PubSub;
}
