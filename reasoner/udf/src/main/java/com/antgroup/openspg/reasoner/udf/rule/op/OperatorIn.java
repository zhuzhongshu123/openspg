/*
 * Copyright 2023 Ant Group CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.
 */
package com.antgroup.openspg.reasoner.udf.rule.op;

import com.ql.util.express.Operator;
import java.lang.reflect.Array;
import java.util.List;

public class OperatorIn extends Operator {
  public OperatorIn(String aName) {
    this.name = aName;
  }

  public OperatorIn(String aAliasName, String aName, String aErrorInfo) {
    this.name = aName;
    this.aliasName = aAliasName;
    this.errorInfo = aErrorInfo;
  }

  @Override
  public Object executeInner(Object[] list) throws Exception {
    Object obj = list[0];
    if (obj == null) {
      // object is null, can not call method
      return false;
    } else if (!((obj instanceof Number) || (obj instanceof String))) {
      return false;
    } else if (list.length == 2 && (list[1].getClass().isArray() || list[1] instanceof List)) {
      if (obj.equals(list[1])) {
        return true;
      } else if (list[1].getClass().isArray()) {
        int len = Array.getLength(list[1]);
        for (int i = 0; i < len; i++) {
          boolean f = OperatorEqualsLessMore.executeInner("==", obj, Array.get(list[1], i));
          if (f) {
            return Boolean.TRUE;
          }
        }
      } else {
        @SuppressWarnings("unchecked")
        List<Object> array = (List<Object>) list[1];
        for (Object o : array) {
          boolean f = OperatorEqualsLessMore.executeInner("==", obj, o);
          if (f) {
            return Boolean.TRUE;
          }
        }
      }
      return false;
    } else if (list.length == 2 && obj instanceof String && list[1] instanceof String) {
      return ((String) list[1]).contains(String.valueOf(obj));
    } else {
      for (int i = 1; i < list.length; i++) {
        boolean f = OperatorEqualsLessMore.executeInner("==", obj, list[i]);
        if (f) {
          return Boolean.TRUE;
        }
      }
      return Boolean.FALSE;
    }
  }
}
