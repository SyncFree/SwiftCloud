package swift.application.swiftdoc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import sys.utils.Threading;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

public class SwiftDocPatchReplay<V> {

	ZipFile zipFile;

	public void parseFiles(SwiftDocOps<V> seq, int delay ) throws Exception {

		SortedSet<ZipEntry> patches = getPatchFiles();

		ZipEntry initial = patches.first();

		if( seq != null )
			seq.begin();
		
		List<Object> doc = new ArrayList<Object>();
		for (String i : fileToLines(initial)) {
						
			if( seq != null)
				seq.add(seq.size(), seq.gen(i) );

			doc.add(i);
		}

		if( seq != null )
			seq.commit() ;
		
		int k = 0;
		for (ZipEntry i : patches) {
			if (i == initial)
				continue;
			
			System.err.printf("\r%s -> %d %% done...", i, 100 * k++ / patches.size() );

			if( seq != null )
				seq.begin();
			
			Patch patch = DiffUtils.parseUnifiedDiff(fileToLines(i));

			List<Object> result = new HelperList<Object>(doc, seq);

			List<Delta> deltas = patch.getDeltas();
			ListIterator<Delta> it = deltas.listIterator(deltas.size());
			while (it.hasPrevious()) {
				Delta delta = (Delta) it.previous();
				delta.applyTo(result);
			}
			doc = result;
			
			if( seq != null )
				seq.commit();

			if( delay > 0 )
			    Threading.sleep(delay);
			
			if( i.getName().startsWith("200-") )
				return;
		}
		
		System.err.println("All Done");
	}

	SortedSet<ZipEntry> getPatchFiles() throws IOException {

		SortedSet<ZipEntry> sortedEntries = new TreeSet<ZipEntry>(new Comparator<ZipEntry>() {
			@Override
			public int compare(ZipEntry a, ZipEntry b) {
				String na = a.getName(), nb = b.getName();
				int s = na.length() - nb.length();
				return s != 0 ? s : na.compareTo(nb);
			}
		});

		File file = new File("swiftdoc-patches.zip") ;
		if( ! file.exists() )
		    file = new File("data/swiftdoc/swiftdoc-patches.zip");
		
		zipFile = new ZipFile( file );

		Enumeration<? extends ZipEntry> e = zipFile.entries();
		while (e.hasMoreElements()) {
			ZipEntry i = e.nextElement();
			sortedEntries.add(i);
		}
		return sortedEntries;
	}
	
	
	public List<String> fileToLines(ZipEntry e) throws IOException {
		List<String> lines = new LinkedList<String>();

		InputStream is = zipFile.getInputStream(e) ;
		BufferedReader in = new BufferedReader(new InputStreamReader( is ) );

		String line;
		while ((line = in.readLine()) != null) {
			lines.add( line );
		}
		in.close();
		is.close();
		return lines;
	}

	class HelperList<T> extends ArrayList<T> {
		private static final long serialVersionUID = 1L;

		final SwiftDocOps<V> mirror;

		HelperList(Collection<T> c, SwiftDocOps<V> mirror) {
			super.addAll(c);
			this.mirror = mirror;
		}

		@Override
		public void add(int i, T v) {
			if (mirror != null)
				mirror.add(i, mirror.gen( v.toString() ));
			super.add(i, v);
		}

		@Override
		public T get(int v) {
			T res = super.get(v);
			if (mirror != null) {
				V res0 = mirror.get(v);
				if (!res0.equals(res)) {
					System.err.printf("%s  got-> %s\n", res, res0);
				}
			}

			return res;
		}

		@Override
		public T remove(int v) {
			T res = super.remove(v);
			if (mirror != null) {
				V res0 = mirror.remove(v);
				if (!res0.equals(res)) {
					System.err.printf("%s  got-> %s\n", res, res0);
				}
			}
			return res;
		}
	}
}
