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

package com.antgroup.openspg.reasoner.parser

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.antgroup.openspg.reasoner.KGDSLParser._
import com.antgroup.openspg.reasoner.common.constants.Constants
import com.antgroup.openspg.reasoner.common.exception.{KGDSLGrammarException, KGDSLOneTaskException}
import com.antgroup.openspg.reasoner.common.graph.edge.Direction
import com.antgroup.openspg.reasoner.common.trees.BottomUp
import com.antgroup.openspg.reasoner.common.types._
import com.antgroup.openspg.reasoner.lube.block._
import com.antgroup.openspg.reasoner.lube.common.expr._
import com.antgroup.openspg.reasoner.lube.common.graph._
import com.antgroup.openspg.reasoner.lube.common.pattern.{Element, GraphPath, PatternElement, PredicateElement}
import com.antgroup.openspg.reasoner.lube.common.rule.{LogicRule, ProjectRule, Rule}
import com.antgroup.openspg.reasoner.lube.parser.ParserInterface
import com.antgroup.openspg.reasoner.lube.utils.{ExprUtils, RuleUtils}
import com.antgroup.openspg.reasoner.lube.utils.transformer.impl.{Expr2QlexpressTransformer, Rule2ExprTransformer}
import com.antgroup.openspg.reasoner.parser.expr.RuleExprParser
import com.antgroup.openspg.reasoner.parser.pattern.{ConceptLabelType, EntityLabelType, PatternParser}

/**
 * parse dsl to Block
 */
class OpenSPGDslParser extends ParserInterface {
  val patternParser: PatternParser = new PatternParser()
  val exprParser: RuleExprParser = new RuleExprParser()
  val expr2StringTransformer = new Expr2QlexpressTransformer()
  val ruleTransformer = new Rule2ExprTransformer()
  var addPropertiesMap: Set[IRProperty] = Set.empty

  /**
   * Get All parameters from dsl, parameter like '${var_name}'
   *
   * @return
   */
  override def getAllParameters(): Set[String] = {
    exprParser.parameters ++ patternParser.exprParser.parameters
  }

  /**
   * get all id filter parameters
   * @return
   */
  override def getIdFilterParameters(): Map[String, String] = {
    exprParser.idFilterParameters ++ patternParser.exprParser.idFilterParameters
  }

  /**
   * Parser kgdsl to block
   *
   * @param text
   * @return
   */
  override def parse(text: String): Block = {
    val blocks = parseMultipleStatement(text)
    if (blocks.size > 1) {
      throw KGDSLOneTaskException("dsl = " + text + "blocks num is " + blocks.size)
    }
    blocks.head
  }

  /**
   * Parser kgdsl Define task and Compute task,
   * will return more than one blocks
   *
   * @param text
   * @param param
   * @return
   */
  override def parseMultipleStatement(
      text: String,
      param: Map[String, Object] = Map.empty): List[Block] = {
    val parser = new LexerInit().initKGReasonerParser(text)
    exprParser.parameters = Set.empty
    exprParser.idFilterParameters = Map.empty
    parseKgDsl(parser.kg_dsl(), if (param == null) Map.empty else param)
  }

  /**
   * Parser dsl to list block
   * @param ctx
   * @return
   */
  def parseKgDsl(ctx: Kg_dslContext, param: Map[String, Object]): List[Block] = {
    var blocks = List[Block]()
    if (ctx.base_predicated_define() != null && ctx.base_predicated_define().size() != 0) {
      for (x <- ctx.base_predicated_define().asScala) {
        addPropertiesMap = Set.empty
        blocks = blocks :+ parseBasePerdicatedDefine(x)
      }
    }
    if (ctx.base_job() != null) {
      blocks :+ parseBaseJob(ctx.base_job(), param)
    } else {
      blocks
    }
  }

  def parseBasePerdicatedDefine(ctx: Base_predicated_defineContext): Block = {
    parseDefine(ctx.the_define_structure())
  }

  def parseDefine(ctx: The_define_structureContext): Block = {
    val ddlBlockWithNodes = parsePerdicatedDefine(ctx.predicated_define())

    val ddlInfo =
      parseBaseRuleDefine(ctx.base_rule_define(), ddlBlockWithNodes._2, ddlBlockWithNodes._3)
    val ddlBlockOp = ddlBlockWithNodes._1.ddlOp.head
    val ruleBlock = ddlInfo._1
    if (ddlInfo._2.nonEmpty) {
      return DDLBlock(ddlInfo._2, List.apply(ruleBlock))
    }
    ddlBlockOp match {
      case AddProperty(s, propertyName, propertyType) =>
        val isLastAssignTargetAlis = ruleBlock match {
          case ProjectBlock(_, projects) =>
            var tmpIsAssign = false
            for (x <- projects.items) {
              if (x._1.name == ddlBlockWithNodes._3.target.alias) {
                tmpIsAssign = true
              }
            }
            tmpIsAssign
          case AggregationBlock(_, aggregations, _) =>
            var tmpIsAssign = false
            for (x <- aggregations.pairs.keySet) {
              if (x.name == ddlBlockWithNodes._3.target.alias) {
                tmpIsAssign = true
              }
            }
            tmpIsAssign
          case _ => false
        }
        if (!isLastAssignTargetAlis) {
          throw new KGDSLGrammarException(
            "add property must assign " +
              ddlBlockWithNodes._3.target.alias + " value at last line")
        }
        val prjBlk = ProjectBlock(
          List.apply(ruleBlock),
          ProjectFields(
            Map.apply(
              IRProperty(s.alias, propertyName) ->
                ProjectRule(
                  IRProperty(s.alias, propertyName),
                  propertyType,
                  Ref(ddlBlockWithNodes._3.target.alias)))))
        DDLBlock(Set.apply(ddlBlockOp), List.apply(prjBlk))
      case AddPredicate(predicate) =>
        var attrFields: Map[String, Ref] = Map.empty
        addPropertiesMap.foreach(x =>
          if (x.name == predicate.alias) {
            attrFields += (x.field -> Ref(generateIRPropertyTmpVariable(x).name))
          })
        val depBlk = ruleBlock

        DDLBlock(
          Set.apply(
            AddPredicate(
              PredicateElement(
                predicate.label,
                predicate.alias,
                predicate.source,
                predicate.target,
                attrFields,
                predicate.direction))),
          List.apply(depBlk))
      case _ => DDLBlock(Set.apply(ddlBlockOp), List.apply(ruleBlock))
    }
  }

  /**
   * Convert a string representation of a type to a type.
   *
   * @param typeName
   * @return
   */
  def parseBasicTypeFromStr(typeName: String): KgType = {
    typeName match {
      case "Int" => KTInteger
      case "String" => KTString
      case "Boolean" => KTBoolean
      case "Long" => KTLong
      case "Float" => KTDouble
      case "Integer" => KTInteger
      case "Text" => KTString
      case _ => KTObject
    }
  }

  // All functions below are parsing and processing functions for ANTLR
  def parsePerdicatedDefine(
      ctx: Predicated_defineContext): Tuple3[DDLBlock, Element, PredicateElement] = {
    val s = patternParser.parseNodePattern(ctx.node_pattern(0))
    val o = patternParser.parseNodePattern(ctx.node_pattern(1))
    val p = patternParser.parseEdgeInfo(
      ctx.full_edge_pointing_right().element_pattern_declaration_and_filler(),
      ctx.full_edge_pointing_right().edge_pattern_pernodelimit_clause(),
      Direction.OUT,
      false)

    val predicateElement =
      PredicateElement(p.relTypes.head, p.alias, s, o, Map.empty, Direction.OUT)
    val basicTypeSet = Set.apply("Int", "String", "Boolean", "Long", "Float", "Text", "Integer")
    if (basicTypeSet.contains(o.typeNames.head)) {
      Tuple3(
        DDLBlock(
          Set.apply(AddProperty(s, p.relTypes.head, parseBasicTypeFromStr(o.typeNames.head))),
          List.empty),
        s,
        predicateElement)
    } else {
      val predicateElement =
        PredicateElement(p.relTypes.head, p.alias, s, o, Map.empty, Direction.OUT)
      Tuple3(DDLBlock(Set.apply(AddPredicate(predicateElement)), List.empty), s, predicateElement)
    }
  }

  def getAllRefVariables(expr: Expr): List[String] = {
    ExprUtils.getRefVariableByExpr(expr)
  }

  def getRefRules(rule: Rule, ruleSet: List[Rule]): List[Rule] = {
    var instructMaps = Map[String, Rule]()
    ruleSet.foreach(rule => {
      instructMaps += (rule.getName -> rule)
    })
    val refs = getAllRefVariables(rule.getExpr)
    if (refs.isEmpty) {
      List.empty
    } else {
      refs.filter(ref => instructMaps.contains(ref)).map(ref => instructMaps(ref))
    }
  }

  def isNeedDependenceExpr(rule: Rule, ruleRefRelate: Map[Rule, Set[Rule]]): Boolean = {
    if (!ruleRefRelate.contains(rule) || ruleRefRelate(rule).size != 1) {
      return false
    }
    val refRule = ruleRefRelate(rule).head
    refRule.getExpr match {
      case _: OrderAndLimit => false
      case _ => true
    }
  }

  def isGenerateOneStepBlockExpr(rule: Rule): Boolean = {
    rule.getExpr match {
      case _: GraphAggregatorExpr => true
      case _: OpChainExpr => true
      case _: OrderAndLimit => true
      case _ => false
    }
  }

  def parseBaseRuleDefine(
      ctx: Base_rule_defineContext,
      head: Element,
      predicate: PredicateElement): (Block, Set[DDLOp]) = {
    val matchBlock = parseGraphStructure(ctx.the_graph_structure(), head, predicate)
    val ruleBlock = parseRule(ctx.the_rule(), matchBlock)
    val ddlOp = parseCreateAction(ctx.create_action())
    (ruleBlock, ddlOp)
  }

  def parseOpChain2Block(
      opChain: OpChainExpr,
      lValueName: IRField,
      graphAggExpr: GraphAggregatorExpr,
      preBlock: Block,
      kg: IRGraph): Block = {
    if (opChain == null) {
      preBlock
    } else {
      var opBlock =
        parseOpChain2Block(opChain.preChainExpr, lValueName, graphAggExpr, preBlock, kg)
      if (graphAggExpr != null && opChain.curExpr == graphAggExpr) {
        return opBlock
      }
      if (opBlock == null) {
        opBlock = preBlock
      }
      if (graphAggExpr != null) {
        val realLValue = lValueName match {
          case null =>
            val aggEleExpr = opChain.curExpr match {
              case AggIfOpExpr(aggOpExpr, _) =>
                aggOpExpr.aggEleExpr
              case AggOpExpr(_, aggEleExpr) =>
                aggEleExpr
              case _ => null
            }
            val refName = aggEleExpr match {
              case Ref(refName) => refName
              case _ => null
            }
            if (kg.edges.contains(refName)) {
              kg.edges(refName)
            } else if (kg.nodes.contains(refName)) {
              kg.nodes(refName)
            } else {
              null
            }
          case x => x
        }
        opChain.curExpr match {
          case Filter(condition) =>
            FilterBlock(
              List.apply(opBlock),
              LogicRule(
                "anonymous_rule_" + patternParser.getDefaultAliasNum,
                "anonymous_rule_" + patternParser.getDefaultAliasNum,
                condition))
          case OrderAndLimit(order, limit) =>
            OrderAndSliceBlock(
              List.apply(opBlock),
              Seq.apply(order match {
                case AscExpr => Asc(limit.column)
                case DescExpr => Desc(limit.column)
              }),
              Option(limit.num),
              graphAggExpr.by.map {
                case Ref(refName) => refName
                case UnaryOpExpr(GetField(fieldName), Ref(refName)) => refName + "." + fieldName
                case x => throw
                  new KGDSLGrammarException("OrderAndSliceBlock can not group " + x.pretty)
              })
          case AggIfOpExpr(_, _) | AggOpExpr(_, _) =>
            if (realLValue == null) {
              throw new KGDSLGrammarException("AggregationBlock generated left variable is null")
            }
            val fieldMap = opBlock.binds.fields.map(f => (f.name, f)).toMap
            AggregationBlock(
              List.apply(opBlock),
              Aggregations(Map.apply(realLValue -> opChain.curExpr.asInstanceOf[Aggregator])),
              graphAggExpr.by.map {
                case Ref(refName) => fieldMap(refName)
                case UnaryOpExpr(GetField(fieldName), Ref(refName)) =>
                  IRProperty(refName, fieldName)
                case x =>
                  throw new KGDSLGrammarException("OrderAndSliceBlock can not group " + x.pretty)
              })
          case _ => throw new KGDSLGrammarException("not support " + opChain.curExpr)
        }
      } else {
        opChain.curExpr match {
          case Filter(condition) =>
            FilterBlock(
              List.apply(opBlock),
              LogicRule(
                "anonymous_rule_" + patternParser.getDefaultAliasNum,
                "anonymous_rule_" + patternParser.getDefaultAliasNum,
                condition))
          case ListOpExpr(name, _) =>
            name match {
              case Constraint(_, _, _) =>
                FilterBlock(
                  List.apply(opBlock),
                  LogicRule(
                    "anonymous_rule_" + patternParser.getDefaultAliasNum,
                    "list filter",
                    opChain))
              case _ =>
                ProjectBlock(
                  List.apply(opBlock),
                  ProjectFields(Map.apply(lValueName ->
                    ProjectRule(lValueName, exprParser.parseRetType(opChain.curExpr), opChain))))
            }
          case AggIfOpExpr(_, _) | AggOpExpr(_, _) =>
            ProjectBlock(
              List.apply(opBlock),
              ProjectFields(
                Map.apply(
                  lValueName ->
                    ProjectRule(
                      lValueName,
                      exprParser.parseRetType(opChain.curExpr),
                      opChain.curExpr))))
          case _ => null
        }
      }

    }
  }

  def generateIRPropertyTmpVariable(p: IRProperty): IRField = {
    IRVariable("tmp_property2variable_prefix_" + p.name + "_" + p.field)
  }

  def parseLValueFiled(irFiled: IRField): IRField = {
    irFiled match {
      case p: IRProperty =>
        addPropertiesMap = addPropertiesMap + p
        generateIRPropertyTmpVariable(p)
      case x => x
    }
  }

  def generateProjectBlock(rule: Rule, preBlock: Block, kg: IRGraph): Block = {
    val lvalueFiled: IRField = rule match {
      case LogicRule(_, _, _) =>
        rule.getExpr match {
          case a: OpChainExpr => null
          case _ => parseLValueFiled(rule.getOutput)
        }
      case _ =>
        parseLValueFiled(rule.getOutput)
    }

    val expr = ruleTransformer.transform(rule)
    expr match {
      case a: OpChainExpr =>
        val aggExpr = a.curExpr match {
          case c: GraphAggregatorExpr => c
          case _ => null
        }
        parseOpChain2Block(a, lvalueFiled, aggExpr, preBlock, kg)
      case a: OrderAndLimit =>
        OrderAndSliceBlock(
          List.apply(preBlock),
          Seq.apply(a.order match {
            case AscExpr => Asc(a.limit.column)
            case DescExpr => Desc(a.limit.column)
          }),
          Option(a.limit.num),
          List.empty)
      case _ =>
        rule match {
          case ProjectRule(_, lvalueType, _) =>
            val projectRule = ProjectRule(lvalueFiled, lvalueType, expr)
            ProjectBlock(
              List.apply(preBlock),
              ProjectFields(Map.apply(lvalueFiled -> projectRule)))
          case _ =>
            ProjectBlock(List.apply(preBlock), ProjectFields(Map.apply(lvalueFiled -> rule)))
        }

    }
  }

  def genBlockOp(
      rule: Rule,
      preBlock: Block,
      filterToProjectBlock: Boolean,
      kg: IRGraph): Block = {
    if (rule.isInstanceOf[LogicRule] && !filterToProjectBlock) {
      FilterBlock(List.apply(preBlock), rule)
    } else {
      generateProjectBlock(rule, preBlock, kg)
    }
  }

  def genBlockOpWithDependency(
      ruleRefRelate: Map[Rule, Set[Rule]],
      rule: Rule,
      preBlock: Block,
      kg: IRGraph): Block = {
    val isGenerateStep = isGenerateOneStepBlockExpr(rule)
    if (isNeedDependenceExpr(rule, ruleRefRelate) && !isGenerateStep) {
      // if ref equal 1, and without graph group we add to dependencies
      ruleRefRelate(rule).head.addDependency(rule)
      null
    } else {
      genBlockOp(
        rule,
        preBlock,
        (ruleRefRelate.contains(rule) && ruleRefRelate(rule).size > 1) || isGenerateStep,
        kg)
    }
  }

  def parseRule(ctx: The_ruleContext, matchBlock: MatchBlock): Block = {
    val nodesProp = new mutable.HashMap[String, IRNode]()
    val edgesProp = new mutable.HashMap[String, IREdge]()
    var refFieldsMap: Map[String, Set[String]] = Map.empty
    var rules: List[Rule] = List.empty
    if (ctx.rule_expression_body().rule_expression().size() != 0) {
      rules = ctx
        .rule_expression_body()
        .rule_expression()
        .asScala
        .map(x => exprParser.parseRuleExpression(x))
        .toList
    }
    parseRuleBlock(rules, matchBlock.patterns)
  }

  def parseRuleBlock(rules: List[Rule], patterns: Map[String, GraphPath]): Block = {
    var refFieldsMap: Map[String, Set[String]] = Map.empty
    if (rules.nonEmpty) {
      rules.foreach(rule => {
        val irFields = RuleUtils.getAllInputFieldInRule(rule, Set.empty, Set.empty)
        irFields.foreach {
          case c: IRNode =>
            var attrs = c.fields
            if (refFieldsMap.contains(c.name)) {
              attrs = attrs ++ refFieldsMap(c.name)
            }
            refFieldsMap += (c.name -> attrs)
          case c => c
        }
      })
    }

    val updatedMatch = patternParser.parseSourceAndMatchBlock(refFieldsMap, patterns)

    var ruleInstructs = Map[Rule, Set[Rule]]()

    rules.foreach(rule => {
      val refRules = getRefRules(rule, rules.toList)
      for (ref <- refRules) {
        if (ruleInstructs.contains(ref)) {
          ruleInstructs += ref -> ruleInstructs(ref).union(Set.apply(rule))
        } else {
          ruleInstructs += (ref -> Set.apply(rule))
        }
      }
    })

    val graph = updatedMatch.dependencies.head.asInstanceOf[SourceBlock].graph
    // reformat
    var curBlock: Block = updatedMatch
    for (rule <- rules) {
      val genBlock = genBlockOpWithDependency(ruleInstructs, rule, curBlock, graph)
      if (genBlock != null) {
        curBlock match {
          case curAggBlock: AggregationBlock if genBlock.isInstanceOf[AggregationBlock] =>
            val genAggBlock = genBlock.asInstanceOf[AggregationBlock]
            val genBlockGroup = genAggBlock.group
            val curBlockGroup = curAggBlock.group
            if (genBlockGroup == curBlockGroup) {
              // same grouping
              curBlock = AggregationBlock(
                curBlock.dependencies,
                Aggregations(genAggBlock.aggregations.pairs ++ curAggBlock.aggregations.pairs),
                genBlockGroup)
            } else {
              curBlock = genBlock
            }
          case _ =>
            curBlock = genBlock
        }
      }
    }
    curBlock
  }

  def parseCreateAction(ctx: Create_actionContext): Set[DDLOp] = {
    if (ctx == null) {
      Set.empty
    } else {
      ctx.create_action_body().asScala.map(x => parseCreateActionBody(x)).toSet
    }
  }

  def parseCreateActionBody(ctx: Create_action_bodyContext): DDLOp = {
    ctx.getChild(0) match {
      case c: Add_edgeContext => parseAddEdge(c)
      case c: Add_nodeContext => parseAddNode(c)
    }
  }

  def parseAddEdge(ctx: Add_edgeContext): AddPredicate = {
    val params = ctx.add_edge_param().asScala.map(x => parseAddEdgeParam(x)).toMap
    AddPredicate(
      PredicateElement(
        parseAddType(ctx.add_type()).head,
        patternParser.getDefaultName,
        parseAddEdgeSrc(params),
        parseAddEdgeDst(params),
        parseAddProps(ctx.add_props()),
        Direction.OUT))
  }

  def parseAddNode(ctx: Add_nodeContext): AddVertex = {
    var alias = patternParser.getDefaultName
    if (ctx.identifier() != null) {
      alias = ctx.identifier().getText
    }
    val element = PatternElement(alias, parseAddType(ctx.add_type()), null)
    AddVertex(element, parseAddProps(ctx.add_props()))
  }

  def parseAddType(ctx: Add_typeContext): Set[String] = {
    if (!ctx.identifier().getText.equals("type")) {
      throw new KGDSLGrammarException("createEdgeInstance/createNodeInstance must has type param")
    }
    patternParser.parseLabelExpress(ctx.label_expression()).map {
      case EntityLabelType(label) => label
      case ConceptLabelType(label, _) => label
    }
  }

  def parseAddEdgeSrc(param: Map[String, String]): Element = {
    if (!param.contains("src")) {
      throw new KGDSLGrammarException("createEdgeInstance must has src param")
    }
    PatternElement(param("src"), Set.empty, null)
  }

  def parseAddEdgeDst(param: Map[String, String]): Element = {
    if (!param.contains("dst")) {
      throw new KGDSLGrammarException("createEdgeInstance must has dst param")
    }
    PatternElement(param("dst"), Set.empty, null)
  }

  def parseAddEdgeParam(ctx: Add_edge_paramContext): (String, String) = {
    (ctx.identifier(0).getText, ctx.identifier(1).getText)
  }

  def parseAddProps(ctx: Add_propsContext): Map[String, Expr] = {
    if (!ctx.identifier().getText.equals("value")) {
      throw new KGDSLGrammarException(
        "createEdgeInstance/createNodeInstance must has value param")
    }
    exprParser.parseComplexExpr(ctx.complex_obj_expr())
  }

  def parseAction(ctx: The_actionContext, preBlock: Block): Block = {
    if (ctx == null) {
      preBlock
    } else {
      parseGetAction(ctx.action_body().get_action(), preBlock)
    }
  }

  def parseActionBody(ctx: Action_bodyContext, preBlock: Block): Block = {
    ctx.getChild(0) match {
      case c: Get_actionContext => parseGetAction(c, preBlock)
      case _ => throw new UnsupportedOperationException(ctx.getChild(0).toString + " not impl")
    }
  }

  def parseGetAndAs(
      ctx: Get_actionContext,
      preBlock: Block): (List[String], List[String], List[Expr], Block, String) = {
    var newPreBlock: Block = preBlock
    var columnNames = List[String]()
    var columnExpr = List[Expr]()
    var outputColumnNames = List[String]()
    val columnProjectRule = ctx
      .one_element_in_get()
      .asScala
      .map(x => {
        val action = parseOneElementInGet(x)
        columnExpr = columnExpr :+ action._1.getExpr
        columnNames = columnNames :+ action._1.getName
        outputColumnNames = outputColumnNames :+ action._2
        if (action._3) {
          action._1
        } else {
          null
        }
      })
      .filter(x => x != null)

    var viewName: String = "view"
    if (ctx.as_view_in_get() != null) {
      if (ctx.as_view_in_get().identifier() != null) {
        viewName = ctx.as_view_in_get().identifier().getText
      }
      outputColumnNames = List[String]()
      ctx
        .as_view_in_get()
        .as_alias_with_comment()
        .asScala
        .foreach(x => {
          outputColumnNames =
            outputColumnNames :+ parseAsAliasWithComment(x, patternParser.getDefaultName)
        })
    }
    newPreBlock = addRuleToBlock(columnProjectRule.toSet, newPreBlock)
    (columnNames, outputColumnNames, columnExpr, newPreBlock, viewName)
  }

  def parseGetAction(ctx: Get_actionContext, preBlock: Block): Block = {
    val (columnNames, outputColumnNames, _, newPreBlock, _) = parseGetAndAs(ctx, preBlock)
    parseTableResultBlock(columnNames, outputColumnNames, Set.empty, List.empty, newPreBlock)
  }

  def parseOneElementInGet(ctx: One_element_in_getContext): (Rule, String, Boolean) = {
    ctx.getChild(0) match {
      case c: One_element_with_variableContext => parseOneElementWithVariable(c)
      case c: One_element_with_constContext => parseOneElementWithConst(c)
      case _ => throw new KGDSLGrammarException("only support get")
    }
  }

  def parseOneElementWithConst(ctx: One_element_with_constContext): (Rule, String, Boolean) = {
    val expr = VString(
      exprParser.parseUnbrokenCharacterStringLiteral(ctx.unbroken_character_string_literal()))
    val defaultName = "const_output_" + patternParser.getDefaultAliasNum
    val columnName = parseAsAliasWithComment(ctx.as_alias_with_comment(), defaultName)
    (ProjectRule(IRVariable(defaultName), KTString, expr), columnName, true)
  }

  def parseGraphStructure(
      ctx: The_graph_structureContext,
      head: Element,
      predicate: PredicateElement): MatchBlock = {
    patternParser.parseGraphStructureDefine(ctx.graph_structure_define(), head, predicate)
  }

  def parseOneElementWithVariable(
      ctx: One_element_with_variableContext): (Rule, String, Boolean) = {
    val expr = exprParser.parseNonParentValueExpressionPrimaryWithProperty(
      ctx.non_parenthesized_value_expression_primary_with_property())
    val defaultColumnName = parseExpr2ElementStr(expr)
    val columnName = parseAsAliasWithComment(ctx.as_alias_with_comment(), defaultColumnName)
    (
      ProjectRule(IRVariable(defaultColumnName), exprParser.parseRetType(expr), expr),
      columnName,
      false)
  }

  def parseAsAliasWithComment(ctx: As_alias_with_commentContext, defaultName: String): String = {
    if (ctx == null) {
      defaultName
    } else {
      ctx.identifier().getText
    }
  }

  /**
   * Convert a expr to output element name
   *
   * @param expr
   * @return
   */
  def parseExpr2ElementStr(expr: Expr): String = {
    expr match {
      case UnaryOpExpr(name, arg) =>
        val express = arg match {
          case Ref(refName) => refName
          case _ =>
            throw new KGDSLGrammarException("Action get must be a variable, not a express")
        }
        val attrName = name match {
          case GetField(fieldName) => fieldName
          case _ => throw new KGDSLGrammarException("Action get must contains attribuate")
        }
        express + "." + attrName
      case Ref(refName) => refName
      case VString(value) => "output_const_" + value
      case _ => throw new KGDSLGrammarException("Action get must be a variable, not a express")
    }
  }

  def addRuleToBlock(rules: Set[Rule], preBlock: Block): Block = {
    var newPreBlock = preBlock
    if (rules.nonEmpty) {
      newPreBlock =
        ProjectBlock(List.apply(preBlock), ProjectFields(rules.map(x => (x.getOutput, x)).toMap))
    }
    newPreBlock
  }

  def parseBaseJob(ctx: Base_jobContext, param: Map[String, Object]): Block = {
    var head: PatternElement = null
    if (param.contains(Constants.START_LABEL)) {
      head = PatternElement(null, Set.apply(param(Constants.START_LABEL).toString), null);
    }

    if (param.contains(Constants.START_ALIAS)) {
      head = PatternElement(param(Constants.START_ALIAS).toString, null, null)
    }
    ctx.getChild(0) match {
      case c: Kgdsl_old_defineContext => parseKgdslOldDefine(c, head)
      case c: Gql_query_statementContext => parseGqlQuery(c, head)
    }
  }

  def parseGqlQuery(ctx: Gql_query_statementContext, head: PatternElement): Block = {
    val matchBlock = parseMatchStatement(ctx.match_statement(), head)
    parseReturn(ctx.return_statement(), matchBlock)
  }

  def parseMatchStatement(ctx: Match_statementContext, head: PatternElement): Block = {
    parseGraphPatternWithWhere(ctx.graph_pattern(), head)
  }

  def parseGraphPatternWithWhere(ctx: Graph_patternContext, head: PatternElement): Block = {
    val paths = patternParser.parsePathPatternList(ctx.path_pattern_list(), head, null)
    var pathMaps = Map[String, GraphPath]()
    paths.foreach(x => {
      if (pathMaps.contains(x.pathName)) {
        pathMaps += (x.pathName -> patternParser.mergeGraphPath(x, pathMaps(x.pathName)))
      } else {
        pathMaps += (x.pathName -> x)
      }
    })

    if (ctx.element_pattern_where_clause() != null) {
      val trans: PartialFunction[Expr, Expr] = {
        case BinaryOpExpr(name, l, r) =>
          name match {
            case BAssign => BinaryOpExpr(BEqual, l, r)
            case x => BinaryOpExpr(x, l, r)
          }
        case x => x
      }
      val expr = BottomUp(trans)
        .transform(patternParser.parseElePatternWhereClause(ctx.element_pattern_where_clause()))

      val rule = LogicRule(
        "anonymous_rule_" + patternParser.getDefaultAliasNum,
        "anonymous_rule_" + patternParser.getDefaultAliasNum,
        expr)
      parseRuleBlock(List.apply(rule), pathMaps)
    } else {
      patternParser.parseSourceAndMatchBlock(Map.empty, pathMaps)
    }
  }

  def parseReturnAlias(ctx: Return_item_aliasContext, defaultName: String): String = {
    if (ctx == null) {
      defaultName
    } else {
      ctx.identifier().getText
    }
  }

  def parseReturnItem(ctx: Return_itemContext): (Rule, String, Boolean) = {
    val expr = exprParser.parseValueExpression(ctx.value_expression())
    val defaultColumnName = parseExpr2ElementStr(expr)
    val columnName = parseReturnAlias(ctx.return_item_alias(), defaultColumnName)
    (
      ProjectRule(IRVariable(defaultColumnName), exprParser.parseRetType(expr), expr),
      columnName,
      false)
  }

  def parseTableResultBlock(
      columnNames: List[String],
      outputColumnNames: List[String],
      rules: Set[Rule],
      graphAttributeInGet: List[String],
      preBlock: Block): Block = {
    if (outputColumnNames.size != columnNames.size) {
      throw new KGDSLGrammarException("as output column not equal get element")
    }
    var dependency = preBlock
    var graph = dependency.transform[IRGraph] {
      case (SourceBlock(graph), _) => graph
      case (_, g) =>
        if (g.isEmpty) {
          null
        } else {
          g.head
        }
    }
    if (graph == null) {
      graph = KG(Map.empty, Map.empty)
    }
    val fieldMap: mutable.HashMap[String, mutable.Set[String]] = getFiledUsedInGraph(columnNames)
    if (null != graphAttributeInGet && graphAttributeInGet.nonEmpty) {
      val extraFieldMap = getFiledUsedInGraph(graphAttributeInGet)
      extraFieldMap.foreach { case (key, newValues) =>
        val combinedValues = fieldMap.getOrElseUpdate(key, new mutable.HashSet[String]())
        combinedValues ++= newValues
      }
    }
    val resultFields = columnNames.map(x =>
      if (x.contains(".")) {
        val part = x.split('.')
        IRProperty(part(0), part(1))
      } else {
        if (graph.nodes.contains(x) || graph.edges.contains(x)) {
          val props = fieldMap.getOrElseUpdate(x, new mutable.HashSet[String]())
          props.add(Constants.PROPERTY_JSON_KEY)
          IRProperty(x, Constants.PROPERTY_JSON_KEY)
        } else if (Constants.GET_PATH_KEY.equals(x)) {
          IRPath(x, (graph.nodes.values ++ graph.edges.values).toList)
        } else {
          IRVariable(x)
        }
      })
    dependency = preBlock.rewriteTopDown {
      case matchBlock: MatchBlock =>
        val mergedMap = (fieldMap /: matchBlock.patterns.head._2.graphPattern.properties) {
          case (acc, (key, values)) =>
            acc.get(key) match {
              case Some(existingValues) =>
                existingValues ++= values
              case None =>
                acc(key) = mutable.Set(values.toSeq: _*)
            }
            acc
        }
        val patterns = matchBlock.patterns.map(tuple =>
          (
            tuple._1,
            tuple._2.copy(graphPattern = tuple._2.graphPattern.copy(properties =
              mergedMap.map(p => (p._1, p._2.toSet)).toMap))))
        matchBlock.copy(patterns = patterns)
      case sourceBlock: SourceBlock =>
        val graph = sourceBlock.graph
        val newNodes = fieldMap
          .filter(k => graph.nodes.keySet.contains(k._1))
          .map(p => (p._1, IRNode(p._1, p._2.toSet)))
          .toMap
        val newEdges = fieldMap
          .filter(k => graph.edges.keySet.contains(k._1))
          .map(p => (p._1, IREdge(p._1, p._2.toSet)))
          .toMap
        sourceBlock.copy(graph = KG(newNodes, newEdges))
    }

    if (rules.isEmpty) {
      TableResultBlock(List.apply(dependency), OrderedFields(resultFields), outputColumnNames)
    } else {
      TableResultBlock(
        List.apply(
          ProjectBlock(
            List.apply(dependency),
            ProjectFields(rules.map(x => (x.getOutput, x)).toMap))),
        OrderedFields(resultFields),
        outputColumnNames)
    }
  }

  def parseReturn(ctx: Return_statementContext, preBlock: Block): Block = {
    var columnNames = List[String]()
    var outputColumnNames = List[String]()
    val columnProjectRule = ctx
      .return_statement_body()
      .return_item_list()
      .return_item()
      .asScala
      .map(x => {
        val action = parseReturnItem(x)
        columnNames = columnNames :+ action._1.getName
        outputColumnNames = outputColumnNames :+ action._2
        if (action._3) {
          action._1
        } else {
          null
        }
      })
      .filter(x => x != null)

    parseTableResultBlock(
      columnNames,
      outputColumnNames,
      columnProjectRule.toSet,
      List.empty,
      preBlock)
  }

  def parseKgdslOldDefine(ctx: Kgdsl_old_defineContext, head: PatternElement): Block = {
    val matchBlock = parseGraphStructure(ctx.the_graph_structure(), head, null)
    val ruleBlock = parseRule(ctx.the_rule(), matchBlock)
    parseAction(ctx.the_action(), ruleBlock)
  }

  /**
   * Rule Parser
   *
   * @param rule
   * @return
   */
  override def parseExpr(rule: String): Expr = exprParser.parse(rule)

  private def getFiledUsedInGraph(
      graphAttributeInGet: List[String]): mutable.HashMap[String, mutable.Set[String]] = {
    val fieldMap = new mutable.HashMap[String, mutable.Set[String]]()
    graphAttributeInGet.map(x =>
      if (x.contains(".")) {
        val part = x.split('.')
        val props = fieldMap.getOrElseUpdate(part(0), new mutable.HashSet[String]())
        props.add(part(1))
      })
    fieldMap
  }

}
