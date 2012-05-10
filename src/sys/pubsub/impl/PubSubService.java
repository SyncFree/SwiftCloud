package sys.pubsub.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import sys.RpcServices;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.pubsub.PubSub;

public class PubSubService extends PubSub {

    private RpcEndpoint svc;
    private RpcFactory factory;
    
    private Map<String, Set<Handler>> localSubscribers;
    private Map<String, Set<Endpoint>> remoteSubscribers;
    
    public PubSubService( RpcFactory fac) {
        this.factory = fac;
        init();        
    }
    
    void init() {  
        localSubscribers = new HashMap<String, Set<Handler>>();
        remoteSubscribers = new HashMap<String, Set<Endpoint>>();
        
        svc = factory.rpcService( RpcServices.PUBSUB.ordinal(), new PubSubRpcHandler(){

            public void onReceive(RpcHandle conn, PubSubNotification m) {
                conn.reply( new PubSubAck( localSubscribers(m.group, false).size() ) );
                
                for( Handler i : localSubscribers(m.group, true)){
                    i.notify(m.group, m.payload);
                }
            }
            
        });
    }
     
    // GC needs work. 
    // Remove a remote subscriber upon first or a certain number of failures.
    // Use ack to reset suspicion?
    public void publish( final String group, final Object payload ) {
        for( Endpoint i : remoteSubscribers(group, true)){
            svc.send( i, new PubSubNotification(group, payload), new PubSubRpcHandler(){
                
               public void onFailure(  RpcHandle handle ) {
            	   removeRemoteSubscriber( group, handle.remoteEndpoint() ) ;
               } 
               
               public void onReceive( final RpcHandle handle, final PubSubAck ack ) {
                   if( ack.totalSubscribers() <= 0) {
                	   removeRemoteSubscriber( group, handle.remoteEndpoint() ) ;
                   }
               }
            });
        }
        
        for( Handler i : localSubscribers(group, true)){
            i.notify(group, payload);
        }
    }
    
    synchronized public void addRemoteSubscriber( String group, Endpoint subscriber ) {
        remoteSubscribers(group, false).add( subscriber);
    }

    synchronized void removeRemoteSubscriber( String group, Endpoint subscriber ) {
        remoteSubscribers(group, false).remove( subscriber);
    }

    synchronized public void subscribe( String group, Handler handler ) {
        localSubscribers( group, false).add( handler);
    }

    synchronized public void unsubscribe( String group, Handler handler ) {
        localSubscribers( group, false).remove( handler);
    }
    
    synchronized private Set<Handler> localSubscribers( String group, boolean clone ) {
        Set<Handler> res = localSubscribers.get( group);
        if( res == null )
            localSubscribers.put( group, res = new HashSet<Handler>() );
        return clone ? new HashSet<Handler>(res) : res ;
    }
    
    synchronized private Set<Endpoint> remoteSubscribers( String group, boolean clone ) {
        Set<Endpoint> res = remoteSubscribers.get( group);
        if( res == null )
            remoteSubscribers.put( group, res = new HashSet<Endpoint>() );
        return clone? new HashSet<Endpoint>(res) : res ;
    }
    
}
