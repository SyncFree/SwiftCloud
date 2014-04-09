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
package swift.application.adservice;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.thoughtworks.xstream.core.util.Base64Encoder;

abstract public class Workload implements Iterable<String>, Iterator<String> {

    static List<String> ads = new ArrayList<String>();
    static List<List<String>> categories = new ArrayList<List<String>>();
    static List<String> adsData = new ArrayList<String>();

    protected Workload() {
    }

    abstract public int size();

    /*
     * Generates random ad names using a fixed, pre-determined random seed to
     * ensure every site generates the same user db.
     */
    public static List<String> populate(int numCategories, int adsPerCategory, int maxViewCount) {
        Random rg = new Random(6L);
        for (int i = 0; i < numCategories; i++) {
            byte[] tmp = new byte[6];
            rg.nextBytes(tmp);
            Base64Encoder enc = new Base64Encoder();
            String category = enc.encode(tmp);
            // Make categories related??
            List<String> relatedCategories = new ArrayList<String>();
            relatedCategories.add(category);
            categories.add(relatedCategories);
            for (int j = 0; j < adsPerCategory; j++) {
                tmp = new byte[6];
                rg.nextBytes(tmp);
                enc = new Base64Encoder();
                String ad = enc.encode(tmp);
                String adLine = String.format("ad title;%s; ad category;%s;view count;%d", ad, category,
                        rg.nextInt(maxViewCount));
                ads.add(ad);
                adsData.add(adLine);
            }
        }
        return adsData;
    }

    /*
     * Represents an abstract command the user performs. Each command has a
     * frequency/probability and need to be formatted into a command line.
     */
    static abstract class Operation {
        int frequency;

        Operation freq(int f) {
            this.frequency = f;
            return this;
        }

        abstract String doLine(Random rg, List<String> candidates);

        public String toString() {
            return getClass().getSimpleName();
        }
    }

    /*
     * View an ad. Target is a randomly chosen from a list of candidates (eg.
     * friends).
     */
    static class ViewAd extends Operation {

        public String doLine(Random rg, List<String> category) {
            return String.format("view_ad;%s", category.get(rg.nextInt(category.size())));
        }
    }

    static Operation[] ops = new Operation[] { new ViewAd().freq(100) };

    static AtomicInteger doMixedCounter = new AtomicInteger(7);

    static public Workload doMixed(int site, final int numCategories, final int adsPerCategory, final int numOps,
            final int sameCategoryFreq, final int number_of_sites) {
        final Random rg = new Random(doMixedCounter.addAndGet(13 + site));
        site = site < 0 ? rg.nextInt(number_of_sites) : site; // fix site

        return new Workload() {
            Iterator<String> it = null;

            void refill() {
                ArrayList<String> group = new ArrayList<String>();
                List<String> currCategories = categories.get(rg.nextInt(numCategories));
                for (int i = 0; i < numOps; i++) {
                    if (rg.nextInt(100) > sameCategoryFreq) {
                        currCategories = categories.get(rg.nextInt(numCategories));
                    }
                    group.add(new ViewAd().doLine(rg, currCategories));
                }

                it = group.iterator();
            }

            @Override
            public boolean hasNext() {
                if (it == null)
                    refill();

                return it.hasNext();
            }

            @Override
            public String next() {
                return it.next();
            }

            @Override
            public void remove() {
                throw new RuntimeException("On demand worload generation; remove is not supported...");
            }

            public int size() {
                return numOps;
            }

        };
    }

    public Iterator<String> iterator() {
        return this;
    }

    public static void main(String[] args) throws Exception {

        Workload.populate(5, 100, 500);

        Workload res = Workload.doMixed(0, 5, 10, 100, 100, 10);
        System.out.println(res.size());
        for (String i : res)
            System.out.println(i);

    }
}
