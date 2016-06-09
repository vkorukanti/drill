/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

public final class PreparedStatementHandle implements Externalizable, Message<PreparedStatementHandle>, Schema<PreparedStatementHandle>
{

    public static Schema<PreparedStatementHandle> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static PreparedStatementHandle getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final PreparedStatementHandle DEFAULT_INSTANCE = new PreparedStatementHandle();

    
    private String sqlQuery;

    public PreparedStatementHandle()
    {
        
    }

    // getters and setters

    // sqlQuery

    public String getSqlQuery()
    {
        return sqlQuery;
    }

    public PreparedStatementHandle setSqlQuery(String sqlQuery)
    {
        this.sqlQuery = sqlQuery;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<PreparedStatementHandle> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public PreparedStatementHandle newMessage()
    {
        return new PreparedStatementHandle();
    }

    public Class<PreparedStatementHandle> typeClass()
    {
        return PreparedStatementHandle.class;
    }

    public String messageName()
    {
        return PreparedStatementHandle.class.getSimpleName();
    }

    public String messageFullName()
    {
        return PreparedStatementHandle.class.getName();
    }

    public boolean isInitialized(PreparedStatementHandle message)
    {
        return true;
    }

    public void mergeFrom(Input input, PreparedStatementHandle message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.sqlQuery = input.readString();
                    break;
                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, PreparedStatementHandle message) throws IOException
    {
        if(message.sqlQuery != null)
            output.writeString(1, message.sqlQuery, false);
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "sqlQuery";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("sqlQuery", 1);
    }
    
}
