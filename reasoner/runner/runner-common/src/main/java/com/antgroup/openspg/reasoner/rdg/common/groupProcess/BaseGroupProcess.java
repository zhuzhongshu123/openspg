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

package com.antgroup.openspg.reasoner.rdg.common.groupProcess;

import com.antgroup.openspg.reasoner.common.exception.NotImplementedException;
import com.antgroup.openspg.reasoner.common.types.KTString$;
import com.antgroup.openspg.reasoner.lube.common.expr.AggUdf;
import com.antgroup.openspg.reasoner.lube.common.expr.Aggregator;
import com.antgroup.openspg.reasoner.lube.common.expr.AggregatorOpSet;
import com.antgroup.openspg.reasoner.lube.common.expr.Expr;
import com.antgroup.openspg.reasoner.lube.logical.PropertyVar;
import com.antgroup.openspg.reasoner.lube.logical.Var;
import com.antgroup.openspg.reasoner.lube.utils.ExprUtils;
import com.antgroup.openspg.reasoner.udf.UdfMngFactory;
import com.antgroup.openspg.reasoner.udf.model.UdafMeta;
import com.antgroup.openspg.reasoner.udf.rule.RuleRunner;
import com.antgroup.openspg.reasoner.warehouse.utils.WareHouseUtils;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import scala.collection.JavaConversions;

public abstract class BaseGroupProcess implements Serializable {
  protected Var var;
  protected UdafMeta udafMeta;
  protected Object[] udfInitParams;
  protected List<String> ruleList;
  protected Aggregator aggOp;
  protected String taskId;

  protected Set<String> exprUseAliasSet;
  protected List<String> exprRuleString;

  /**
   * Construct from var and aggregator
   *
   * @param taskId
   * @param var
   * @param aggregator
   */
  public BaseGroupProcess(String taskId, Var var, Aggregator aggregator) {
    this.taskId = taskId;
    this.var = var;
    this.aggOp = aggregator;
    this.ruleList = parseRuleList();
    this.udfInitParams = parseUdfInitParams();
    this.udafMeta = parseUdafMeta();

    this.exprUseAliasSet = parseExprUseAliasSet();
    this.exprRuleString = parseExprRuleList();
  }

  /**
   * judge is first agg function
   *
   * @return
   */
  public boolean notPropertyAgg() {
    return (!(var instanceof PropertyVar));
  }

  /**
   * get udaf str name from op
   *
   * @param op
   * @return
   */
  public String getUdafStrName(AggregatorOpSet op) {
    if (op instanceof AggUdf) {
      AggUdf aggUdf = (AggUdf) op;
      return aggUdf.name();
    } else {
      return op.toString();
    }
  }

  private Object[] getUdafInitializeParams(List<Expr> exprList) {
    Object[] params = new Object[exprList.size()];
    for (int i = 0; i < exprList.size(); ++i) {
      Expr expr = exprList.get(i);
      List<String> paramRuleList = WareHouseUtils.getRuleList(expr);
      Object value =
          RuleRunner.getInstance().executeExpression(new HashMap<>(), paramRuleList, this.taskId);
      params[i] = value;
    }
    return params;
  }

  protected Object[] parseUdfInitParams() {
    Object[] udfInitParams = null;
    AggregatorOpSet aggregatorOpSet = getAggOpSet();
    if (aggregatorOpSet instanceof AggUdf) {
      AggUdf aggUdf = (AggUdf) aggregatorOpSet;
      udfInitParams = getUdafInitializeParams(JavaConversions.seqAsJavaList(aggUdf.funcArgs()));
    }
    return udfInitParams;
  }

  protected UdafMeta parseUdafMeta() {
    String udafName = getUdafStrName(getAggOpSet());
    UdafMeta udafMeta = UdfMngFactory.getUdfMng().getUdafMeta(udafName, KTString$.MODULE$);
    if (udafMeta == null) {
      throw new NotImplementedException("unsupported aggregator function, type=" + udafName, null);
    }
    return udafMeta;
  }

  /**
   * parse rule list by op
   *
   * @return
   */
  protected abstract List<String> parseRuleList();

  /**
   * get agg op set by op
   *
   * @return
   */
  public abstract AggregatorOpSet getAggOpSet();

  /**
   * get agg ele by op
   *
   * @return
   */
  public abstract Expr getAggEle();

  public Set<String> parseExprUseAliasSet() {
    scala.collection.immutable.List<String> aliasList = ExprUtils.getRefVariableByExpr(getAggEle());
    return new HashSet<>(JavaConversions.seqAsJavaList(aliasList));
  }

  public List<String> parseExprRuleList() {
    return WareHouseUtils.getRuleList(getAggEle());
  }

  /**
   * getter
   *
   * @return
   */
  public Var getVar() {
    return var;
  }

  /**
   * getter
   *
   * @return
   */
  public UdafMeta getUdafMeta() {
    return udafMeta;
  }

  /**
   * getter
   *
   * @return
   */
  public Object[] getUdfInitParams() {
    return udfInitParams;
  }

  /**
   * getter
   *
   * @return
   */
  public List<String> getRuleList() {
    return ruleList;
  }

  public Set<String> getExprUseAliasSet() {
    return exprUseAliasSet;
  }

  public List<String> getExprRuleString() {
    return exprRuleString;
  }
}
