/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.expressions;

import org.junit.Test;

public class ExpressionParserTest {
  @Test
  public void predicateTest() {
    UnboundPredicate expression = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column-Name"),
            Literal.of(50));

    System.out.println(ExpressionParser.toJson(expression, true));
  }
  @Test
  public void andTest() {
    UnboundPredicate operandOne = new UnboundPredicate(
            Expression.Operation.GT_EQ,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    UnboundPredicate operandTwo = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column2-Name"),
            Literal.of(50));

    And expression = new And(operandOne, operandTwo);
    System.out.println(ExpressionParser.toJson(expression, true));
  }

  @Test
  public void orTest() {
    UnboundPredicate operandOne = new UnboundPredicate(
            Expression.Operation.LT,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    UnboundPredicate operandTwo = new UnboundPredicate(
            Expression.Operation.NOT_NULL,
            new NamedReference("Column2-Name"));

    Or expression = new Or(operandOne, operandTwo);
    System.out.println(ExpressionParser.toJson(expression, true));
  }

  @Test
  public void notTest() {
    UnboundPredicate operandOne = new UnboundPredicate(
            Expression.Operation.LT,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    Not expression = new Not(operandOne);
    System.out.println(ExpressionParser.toJson(expression, true));
  }

  @Test
  public void nestedExpressionTest() {
    UnboundPredicate test1 = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    UnboundPredicate test2 = new UnboundPredicate(
            Expression.Operation.EQ,
            new NamedReference("Column2-Name"),
            Literal.of("Test"));

    UnboundPredicate test3 = new UnboundPredicate(
            Expression.Operation.LT,
            new NamedReference("Column3-Name"),
            Literal.of(80));

    UnboundPredicate test4 = new UnboundPredicate(
            Expression.Operation.IS_NAN,
            new NamedReference("Column4-Name"));

    And expression1 = new And(test1, test2);
    Or expression2 = new Or(expression1, test3);
    And expression3 = new And(expression2, test4);
    System.out.println(ExpressionParser.toJson(expression3, true));
  }
}
