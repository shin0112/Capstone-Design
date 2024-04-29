/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.couchdb;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.AbstractList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchRules {
  // TODO : Rule 정의 후 INSTANCE 추가
  public static final RelOptRule[] RULES = {CouchProjectRule.INSTANCE,};
  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();
  private CouchRules() {
  }

  static List<String> couchFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(new AbstractList<String>() {
      @Override
      public String get(int index) {
        final String name = rowType.getFieldList().get(index).getName();
        return name.startsWith("$") ? "_" + name.substring(2) : name;
      }

      @Override
      public int size() {
        return rowType.getFieldCount();
      }
    }, SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  static String maybeQuote(String s) {
    if (!needsQuote(s)) {
      return s;
    }
    return quote(s);
  }

  static String quote(String s) {
    return "'" + s + "'"; // TODO: handle embedded quotes
  }

  private static boolean needsQuote(String s) {
    for (int i = 0, n = s.length(); i < n; i++) {
      char c = s.charAt(i);
      if (!Character.isJavaIdentifierPart(c)
          || c == '$') {
        return true;
      }
    }
    return false;
  }

  /**
   * Translator from {@link RexNode} to strings in CouchDB's expression
   * language.
   */
  static class RexToCouchTranslator extends RexVisitorImpl<String> {
    private static final Map<SqlOperator, String> COUCH_OPERATORS = new HashMap<>();

    static {
      // Arithmetic
      COUCH_OPERATORS.put(SqlStdOperatorTable.DIVIDE, "$divide");
      COUCH_OPERATORS.put(SqlStdOperatorTable.MULTIPLY, "$multiply");
      COUCH_OPERATORS.put(SqlStdOperatorTable.MOD, "$mod");
      COUCH_OPERATORS.put(SqlStdOperatorTable.PLUS, "$add");
      COUCH_OPERATORS.put(SqlStdOperatorTable.MINUS, "$subtract");
      // Boolean
      COUCH_OPERATORS.put(SqlStdOperatorTable.AND, "$and");
      COUCH_OPERATORS.put(SqlStdOperatorTable.OR, "$or");
      COUCH_OPERATORS.put(SqlStdOperatorTable.NOT, "$not");
      // Comparison
      COUCH_OPERATORS.put(SqlStdOperatorTable.EQUALS, "$eq");
      COUCH_OPERATORS.put(SqlStdOperatorTable.NOT_EQUALS, "$ne");
      COUCH_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN, "$gt");
      COUCH_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "$gte");
      COUCH_OPERATORS.put(SqlStdOperatorTable.LESS_THAN, "$lt");
      COUCH_OPERATORS.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "$lte");
    }

    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToCouchTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }


  }

  abstract static class CouchConverterRule extends ConverterRule {
    protected CouchConverterRule(Config config) {
      super(config);
    }
  }

  /**
   * Rule to convert a {@link LogicalProject}
   * to a {@link CouchProject}.
   */
  private static class CouchProjectRule extends CouchConverterRule {
    static final CouchProjectRule INSTANCE =
        Config.INSTANCE.withConversion(LogicalProject.class, Convention.NONE,
            CouchRel.CONVENTION, "CouchProjectRule").withRuleFactory(CouchProjectRule::new).toRule(CouchProjectRule.class);

    CouchProjectRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      return project.getVariablesSet().isEmpty();
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new CouchProject(project.getCluster(), traitSet, convert(project.getInput(), out),
          project.getProjects(), project.getRowType());
    }
  }
}
