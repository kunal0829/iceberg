/*
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
 */

/*
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
 */

/*
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
 */

/*
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
 */

package org.apache.iceberg.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Test;

public class ExpressionParserTest {
  @Test
  public void parserTest() {

    UnboundPredicate test1 = new UnboundPredicate(Expression.Operation.IN, new NamedReference("Test"), Literal.of(34));
    UnboundPredicate test2 = new UnboundPredicate(Expression.Operation.EQ, new NamedReference("Test2"), Literal.of("Test"));
    UnboundPredicate test3 = new UnboundPredicate(Expression.Operation.LT, new NamedReference("Test3"), Literal.of(80));
    UnboundPredicate test4 = new UnboundPredicate(Expression.Operation.IS_NULL, new NamedReference("Test"));

    And expression1 = new And(test1, test2);
    Or expression2 = new Or(expression1, test3);
    Not expression3 = new Not(test1);

    //System.out.println(ExpressionParser.toJson(expression2));
    System.out.println(ExpressionParser.toJson(expression1));
  }
}
