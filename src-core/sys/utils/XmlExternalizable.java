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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import com.thoughtworks.xstream.XStream;

public class XmlExternalizable {

    public void saveXmlTo(String name) throws Exception {
        File tmp = new File(name + ".tmp");
        if (!tmp.exists()) {
            tmp.getParentFile().mkdirs();
        }
        FileOutputStream fos = new FileOutputStream(tmp);
        XStream xs = new XStream();
        xs.toXML(this, fos);
        fos.close();
        File fok = new File(name);
        if (tmp.exists())
            tmp.renameTo(fok);
        xs = null;
    }

    @SuppressWarnings("unchecked")
    static public <T> T loadFromXml(String name) throws Exception {
        File f = new File(name);
        FileInputStream fis = new FileInputStream(f);
        XStream xs = new XStream();
        Object res = xs.fromXML(fis);
        xs = null;
        return (T) res;
    }
}
