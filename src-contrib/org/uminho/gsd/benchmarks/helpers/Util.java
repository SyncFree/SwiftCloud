

package org.uminho.gsd.benchmarks.helpers;

import java.util.Random;

/**
 * Various helpful utility functions.
 * 
 * @author <a href="mailto:totok@cs.nyu.edu">Alexander Totok</a>
 * 
 * @version   $Revision: 1.4 $   $Date: 2005/02/05 21:26:28 $   $Author: totok $
 */
public class Util {

	public static final String getRandomAString(Random rand, int length){
		String newstring = new String();
		for(int i = 0; i < length; i++){
			char c = Constants.CHARS[(int) Math.floor(rand.nextDouble()*Constants.NUM_CHARS)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}

	public static final String getRandomAString(Random rand, int min, int max) {
		String str = new String();
		int strlen = (int) Math.floor(rand.nextDouble()*((max-min)+1));
		strlen += min;
		for (int i = 0; i < strlen; i++){
			char c = Constants.CHARS[(int)Math.floor(rand.nextDouble()*Constants.NUM_CHARS)];
			str = str.concat(String.valueOf(c));
		}
		return str;
	}
    
	public static final int getRandomInt(Random rand, int lower, int upper){
		int num = (int)Math.floor(rand.nextDouble()*(upper-lower+1));
		return num + lower;
	}

	public static final String getRandomNString(Random rand, int length) {
		String newstring = new String();
		for (int i = 0; i < length; i++){
			char c = Constants.NUMBERS[(int)Math.floor(rand.nextDouble()*10)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}

	public static final String getRandomNString(Random rand, int min, int max) {
		String str = new String();
		int strlen = (int) Math.floor(rand.nextDouble()*((max-min)+1));
		strlen += min;
		for (int i = 0; i < strlen; i++){
			char c = Constants.NUMBERS[(int)Math.floor(rand.nextDouble()*10)];
			str = str.concat(String.valueOf(c));
		}
		return str;
	}

	public static final String DigSyl(Random rand, int D, int N) {
		String resultString = new String();
		String Dstr = Integer.toString(D);

		if (N > Dstr.length()) {
			int padding = N - Dstr.length();
			for (int i = 0; i < padding; i++) resultString = resultString.concat("BA");
		}
	
		for (int i = 0; i < Dstr.length(); i++){
			if (Dstr.charAt(i) == '0')
				resultString = resultString.concat("BA");
			else if(Dstr.charAt(i) == '1')
				resultString = resultString.concat("OG");
			else if(Dstr.charAt(i) == '2')
				resultString = resultString.concat("AL");
			else if(Dstr.charAt(i) == '3')
				resultString = resultString.concat("RI");
			else if(Dstr.charAt(i) == '4')
				resultString = resultString.concat("RE");
			else if(Dstr.charAt(i) == '5')
				resultString = resultString.concat("SE");
			else if(Dstr.charAt(i) == '6')
				resultString = resultString.concat("AT");
			else if(Dstr.charAt(i) == '7')
				resultString = resultString.concat("UL");
			else if(Dstr.charAt(i) == '8')
				resultString = resultString.concat("IN");
			else if(Dstr.charAt(i) == '9')
				resultString = resultString.concat("NG");
		}
		return resultString;
	}
}
