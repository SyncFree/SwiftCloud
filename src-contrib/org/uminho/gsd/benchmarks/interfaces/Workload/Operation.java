/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.interfaces.Workload;

import java.util.Map;

/**
 * An operation contains the description of an operation to be executed by the database executor.
 * Its fields represent the operation name and parameters.
 */
public class Operation {

    //The operation name
    private String operation;

    //Parameters
    private Map<String, Object> parameters;

    //Operation result
    private Object result;

    public Operation(String operation, Map<String, Object> parameters) {
        this.operation = operation;
        this.parameters = parameters;
    }

    public String getOperation() {
        return operation;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }


    public Object getParameter(String name) throws NoSuchFieldException {
        if (parameters.containsKey(name)) {
            return parameters.get(name);
        } else {
            throw new NoSuchFieldException(name);
        }


    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }
}
