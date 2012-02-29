package sys.dht.catadupa;

import java.util.* ;

import static sys.Sys.Sys;

/**
 * 
 * @author smd
 *
 * @param <T>
 */
public class RandomList<T> extends ArrayList<T>{

	public RandomList() {}

	public RandomList( Collection <? extends T> c) {
		super(c) ;
	}
	
	public RandomList( Iterator <? extends T> it ) {
		for( ; it.hasNext() ; )
			add( it.next() ) ;
	}
	
	public T randomElement() {
		return isEmpty() ? null : get( Sys.rg.nextInt( super.size() ) ) ;
	}
	
	public T removeRandomElement() {
		return isEmpty() ? null : remove( Sys.rg.nextInt( super.size() ) ) ;		
	}		
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}
