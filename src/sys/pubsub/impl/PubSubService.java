package sys.pubsub.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import sys.RpcServices;
import sys.net.api.Endpoint;
import sys.net.api.rpc.RpcConnection;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcFactory;
import sys.pubsub.PubSub;

import sys.net.api.rpc.RpcMessage;

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

            public void onReceive(RpcConnection conn, PubSubNotification m) {
                conn.reply( new PubSubAck() );
                
                for( Handler i : localSubscribers(m.group, true)){
                    i.notify(m.group, m.payload);
                }
            }
            
        });
    }
     
    // GC needs work. 
    // Remove a remote subscribe upon first or a certain number of failures.
    // Use ack to reset suspicion?
    public void publish( final String group, final Object payload ) {
        for( Endpoint i : remoteSubscribers(group, true)){
            svc.send( i, new PubSubNotification(group, payload), new PubSubRpcHandler(){
                
               public void onFailure( Endpoint dst, RpcMessage m) {
                   synchronized(this) {
                       remoteSubscribers( group, false).remove( dst);
                   }
               } 
               
               public void onReceive( final PubSubAck ack ) {
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
    
    synchronized public void subscribe( String group, Handler handler ) {
        localSubscribers( group, false).add( handler);
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
