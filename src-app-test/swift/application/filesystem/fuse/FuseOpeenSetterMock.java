/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 Kaiserslautern University of Technology
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
/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package swift.application.filesystem.fuse;

import fuse.FuseOpenSetter;
import swift.application.filesystem.IFile;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class FuseOpeenSetterMock implements FuseOpenSetter{
    IFile file;
    @Override
    public void setFh(Object o) {
        file=(IFile)o;
    }

    public IFile getFile() {
        return file;
    }
    

    @Override
    public boolean isDirectIO() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setDirectIO(boolean bln) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isKeepCache() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setKeepCache(boolean bln) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
