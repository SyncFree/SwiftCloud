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

/**
 * Provides methods for generating CRDT Identifiers based on the class and type
 * of object.
 * 
 * @author annettebieniusa
 * 
 */

public class NamingScheme {

    public static CRDTIdentifier forAd(String adTitle) {
        return new CRDTIdentifier("ads", adTitle);
    }

    public static CRDTIdentifier forViewCount(String adTitle) {
        return new CRDTIdentifier("ads_count", adTitle);
    }

    public static CRDTIdentifier forCategory(String category) {
        return new CRDTIdentifier("ads_category", category);
    }

}
