package swift.application.swiftdoc;

public interface SwiftDocOps<V> {

	V gen( String s );
	
	void begin();
	
	void add(int i, V v) ;

	V get(int v) ;

	public V remove(int v) ;
	
	int size();
	
	void commit();
}

