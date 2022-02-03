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
  public void parserTest() {
    UnboundPredicate test1 = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Test"),
            Literal.of(34));

    UnboundPredicate test2 = new UnboundPredicate(
            Expression.Operation.EQ,
            new NamedReference("Test2"),
            Literal.of("Test"));

    UnboundPredicate test3 = new UnboundPredicate(
            Expression.Operation.LT,
            new NamedReference("Test3"),
            Literal.of(80));

    And expression1 = new And(test1, test2);
    Or expression2 = new Or(expression1, test3);
    And expression3 = new And(expression1, expression2);

    System.out.println(ExpressionParser.toJson(expression3, true));
  }
}
