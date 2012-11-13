package sys.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import com.thoughtworks.xstream.XStream;


public class XmlExternalizable {

	public void saveXmlTo(String name) throws Exception {	
		File tmp = new File( name + ".tmp") ;
		if( ! tmp.exists() ) {
			tmp.getParentFile().mkdirs() ;
		}
		FileOutputStream fos = new FileOutputStream( tmp ) ;		
		XStream xs = new XStream() ;
		xs.toXML( this, fos ) ;
		fos.close() ;			
		File fok = new File( name ) ;
		if( tmp.exists() )
			tmp.renameTo( fok ) ;
		xs = null ;
	}
	
	@SuppressWarnings("unchecked")
	static public <T> T loadFromXml( String name) throws Exception {
		File f = new File( name ) ;
		FileInputStream fis = new FileInputStream( f ) ;
		XStream xs = new XStream() ;
		Object res = xs.fromXML(fis) ;
		xs = null ;
		return (T) res ;
	}
}
