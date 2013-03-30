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
package swift.application.swiftdoc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class SwiftDocPatchReplay<V> {

    ZipFile zipFile;

    public void parseFiles(SwiftDocOps<V> seq) throws Exception {

        SortedSet<ZipEntry> patches = getPatchFiles();

        ZipEntry initial = patches.first();

        // Populate initial doc with
        List<Object> doc = new ArrayList<Object>();
        for (String i : fileToLines(initial)) {
            if (seq != null) {
                seq.begin();
                seq.add(seq.size(), seq.gen(i));
                seq.commit();
            }
            doc.add(i);
        }

        int k = 0;
        for (ZipEntry i : patches) {
            if (i == initial)
                continue;

            System.err.printf("\r%s -> %d %% done...", i, 100 * k++ / patches.size());

            if (seq != null)
                seq.begin();

            doc.add(i.getName());
            seq.add(0, seq.gen(i.getName()));
            // Patch patch = DiffUtils.parseUnifiedDiff(fileToLines(i));
            //
            // List<Object> result = new HelperList<Object>(doc, seq);
            //
            // List<Delta> deltas = patch.getDeltas();
            // ListIterator<Delta> it = deltas.listIterator(deltas.size());
            // while (it.hasPrevious()) {
            // Delta delta = (Delta) it.previous();
            // delta.applyTo(result);
            // }
            // doc = result;
            if (seq != null)
                seq.commit();

            if (i.getName().startsWith("500-"))
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

        File file = new File("swiftdoc-patches.zip");
        if (!file.exists())
            file = new File("evaluation/swiftdoc/swiftdoc-patches.zip");

        zipFile = new ZipFile(file);

        Enumeration<? extends ZipEntry> e = zipFile.entries();
        while (e.hasMoreElements()) {
            ZipEntry i = e.nextElement();
            sortedEntries.add(i);
        }
        return sortedEntries;
    }

    public List<String> fileToLines(ZipEntry e) throws IOException {
        List<String> lines = new LinkedList<String>();

        InputStream is = zipFile.getInputStream(e);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));

        String line;
        while ((line = in.readLine()) != null) {
            lines.add(line);
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
                mirror.add(i, mirror.gen(v.toString()));
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
