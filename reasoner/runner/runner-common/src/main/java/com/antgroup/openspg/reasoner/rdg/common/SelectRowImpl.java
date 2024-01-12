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

package com.antgroup.openspg.reasoner.rdg.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.antgroup.openspg.reasoner.common.constants.Constants;
import com.antgroup.openspg.reasoner.common.graph.vertex.IVertexId;
import com.antgroup.openspg.reasoner.common.table.FieldType;
import com.antgroup.openspg.reasoner.common.types.KTString$;
import com.antgroup.openspg.reasoner.common.types.KgType;
import com.antgroup.openspg.reasoner.kggraph.KgGraph;
import com.antgroup.openspg.reasoner.lube.common.pattern.Pattern;
import com.antgroup.openspg.reasoner.lube.logical.PathVar;
import com.antgroup.openspg.reasoner.lube.logical.PropertyVar;
import com.antgroup.openspg.reasoner.lube.logical.Var;
import com.antgroup.openspg.reasoner.utils.RunnerUtil;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SelectRowImpl implements Serializable {
  private final List<Var> columns;
  private final Map<String, Object> initRuleContext;
  private final boolean forceOutputString;

  public SelectRowImpl(List<Var> columns, Pattern kgGraphSchema, boolean forceOutputString) {
    this.columns = columns;
    this.initRuleContext = RunnerUtil.getKgGraphInitContext(kgGraphSchema);
    this.forceOutputString = forceOutputString;
  }

  public Object[] toRow(KgGraph<IVertexId> path) {
    Map<String, Object> context = RunnerUtil.kgGraph2Context(this.initRuleContext, path);
    Object[] row = new Object[columns.size()];
    for (int i = 0; i < this.columns.size(); ++i) {
      Var var = this.columns.get(i);
      Object selectValue;
      KgType fieldType;
      if (var instanceof PathVar) {
        selectValue = getSelectValue(null, Constants.GET_PATH_KEY, context);
        fieldType = KTString$.MODULE$;
      } else {
        PropertyVar propertyVar = (PropertyVar) var;
        selectValue = getSelectValue(propertyVar.name(), propertyVar.field().name(), context);
        fieldType = propertyVar.field().kgType();
      }

      if (null == selectValue) {
        row[i] = null;
      } else if (this.forceOutputString) {
        row[i] = String.valueOf(selectValue);
      } else {
        FieldType type = FieldType.fromKgType(fieldType);
        if (FieldType.STRING.equals(type)) {
          row[i] = String.valueOf(selectValue);
        } else if (FieldType.INT.equals(type) || FieldType.LONG.equals(type)) {
          if (selectValue instanceof Long) {
            row[i] = selectValue;
          } else {
            row[i] = Long.valueOf(String.valueOf(selectValue));
          }
        } else if (FieldType.DOUBLE.equals(type)) {
          if (selectValue instanceof Double) {
            row[i] = selectValue;
          } else {
            row[i] = Double.valueOf(String.valueOf(selectValue));
          }
        } else if (FieldType.BOOLEAN.equals(type)) {
          if (selectValue instanceof Boolean) {
            row[i] = selectValue;
          } else {
            row[i] = Boolean.valueOf(String.valueOf(selectValue));
          }
        } else {
          row[i] = selectValue;
        }
      }
    }
    return row;
  }

  private Object getSelectValue(String alias, String propertyName, Map<String, Object> context) {
    if (Constants.PROPERTY_JSON_KEY.equals(propertyName)) {
      Object propertyMap = context.get(alias);
      return JSON.toJSONString(
          propertyMap,
          SerializerFeature.PrettyFormat,
          SerializerFeature.DisableCircularReferenceDetect,
          SerializerFeature.SortField);
    } else if (Constants.GET_PATH_KEY.equals(propertyName)) {
      return JSON.toJSONString(
          context,
          SerializerFeature.PrettyFormat,
          SerializerFeature.DisableCircularReferenceDetect,
          SerializerFeature.SortField);
    }
    Map<String, Object> propertyMap = (Map<String, Object>) context.get(alias);
    return propertyMap.get(propertyName);
  }
}
