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

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.Copyable;

public class Ad implements Copyable {
    CRDTIdentifier adId;
    String title;
    String category;
    int maximumViews;

    CRDTIdentifier viewCount;

    /** DO NOT USE: Empty constructor needed for Kryo */
    public Ad() {
    }

    public Ad(final String title, final String category, int maximumViews) {
        this.adId = NamingScheme.forAd(title);
        this.title = title;
        this.category = category;
    }

    @Override
    public Object copy() {
        Ad copyObj = new Ad(title, category, maximumViews);
        return copyObj;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(title);
        sb.append("AD: ");
        sb.append(title);
        sb.append(" CAT ");
        sb.append(category);
        return sb.toString();
    }

}
