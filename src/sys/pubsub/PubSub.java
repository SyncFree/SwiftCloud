package sys.pubsub;

import sys.net.api.Endpoint;

abstract public class PubSub<K,P> {

	public interface Handler<K,P> {
		void notify( final K key, final P info);
	}

	public abstract void publish(K key, P info);

	public abstract void subscribe(K key, Handler<K,P> handler);

	public abstract void unsubscribe(K key, Handler<K,P> handler);
	
	public abstract void addRemoteSubscriber(K key, Endpoint subscriber);
}
