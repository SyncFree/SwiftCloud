/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.utils;

import java.io.*;

public class SlidingIntSet implements Serializable {

	int base ;
	long[] bits ;
	
	public SlidingIntSet() {
		this(2) ;
	}

	public SlidingIntSet( int initialSize ) {
		this.base = 0 ;
		this.bits = new long[ initialSize ] ;
	}

	public SlidingIntSet( SlidingIntSet other ) {
		this.base = other.base ;
		this.bits = new long[ other.bits.length ] ;
		System.arraycopy( other.bits, 0, bits, 0, bits.length) ;
	}
	
	public SlidingIntSet clone() {
		return new SlidingIntSet( this ) ;
	}
	
	protected SlidingIntSet( int base, long[] bits ) {
		this.base = base ;
		this.bits = bits ;
		while( bits[0] == ALL_ONES )
			slide() ;
	}
	
	public int base() {
		return base ;
	}
	
	public int length() {
		return base + bits.length << 6 ;
	}
	
	int window() {
		return bits.length << 6 ;
	}
	
	public SlidingIntSet set( int b ) {
		try {		
			int i = b - base ;
			if( i >= 0 ) {
				bits[ i >> 6 ] |= masks[ i & 63 ] ;		
				while( bits[0] == ALL_ONES ) 
					slide() ;
			}
		} catch( Exception x ) {
			grow() ;
			set( b ) ;
		}
		return this ;
	}
	
	public boolean get( int b ) {
		int j, i = b - base ;
		return i < 0 || ( (j = (i >> 6)) < bits.length && (bits[j] & masks[i & 63]) != 0) ;	
	}
	
	public boolean equals( SlidingIntSet other ) {
		int b = Math.min( this.base, other.base ) ;
		int B = Math.max( this.base + (this.bits.length << 6), other.base + (other.bits.length << 6)) ;
		int l = (B - b) >> 6 ;
		for( int i = b ; i < l ; i++ )
			if( this.word(i) != other.word(i) ) return false ;
		return true ;
	}
	
	private long word( int b ) {
		int i = b - base ;
		if( i < 0 ) return ALL_ONES ;
		else if( i >= bits.length << 6 ) return 0L ;
		else return bits[ i >> 6 ] ;
	}
	
	public SlidingIntSet union( SlidingIntSet other ) {
		int b = Math.max( this.base, other.base ) ;
		int B = Math.max( this.base + (this.bits.length << 6), other.base + (other.bits.length << 6)) ;
		int l = (B - b) >> 6 ;
		long[] new_bits = new long[l] ;
		for( int i = 0 ; i < l ; i++ ) {
			int bb = b + (i << 6) ;	
			new_bits[i] = this.word( bb) | other.word( bb) ; 
		}
		return new SlidingIntSet( b, new_bits ) ;
	}
	
	public boolean contains( SlidingIntSet other ) {
		return this.equals( other.union(this)) ;
	}
	
	public String toString() {
		String res = "" ;
		for( int i = 0 ; i < bits.length ; i++ ) {
			long v = bits[i] ;
			for( int j = 64 ; --j >= 0 ; ) {
				long m = (1L << j) & v ;
				res = res + (m == 0 ? "0" : "1") ;
			}
		}
		return String.format("<%d : %s>", base, res ) ;
	}
	
	final private void slide() {
		base += 64 ;
		for( int j = 0 ; j < bits.length - 1 ; j++ )
			bits[j] = bits[j+1] ;				
		bits[ bits.length - 1 ] = 0L ;
	} 
	
	final private void grow() {
		long[] new_bits = new long[ 2 * bits.length ] ;
		System.arraycopy( bits, 0, new_bits, 0, bits.length ) ;
		bits = new_bits ;
	}
	
	private static long[] masks = new long[64];
	private static final long ALL_ONES = 0xFFFFFFFFFFFFFFFFL ;
	static {
		for( int i = 0 ; i < 64 ; i++ )
			masks[i] = 0x8000000000000000L >>> i ;
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}
