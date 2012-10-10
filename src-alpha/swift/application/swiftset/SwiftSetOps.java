package swift.application.swiftset;

public interface SwiftSetOps<V> {

	V gen( String s );
	
	void begin();
	
	void add(V v) ;

	public void remove(V v) ;
	
	int size();
	
	void commit();
}

