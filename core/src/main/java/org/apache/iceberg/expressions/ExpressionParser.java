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
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.List;
import com.fasterxml.jackson.core.JsonGenerator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.util.JsonUtil;


public class ExpressionParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String OPERATION = "operation";
  private static final String TERM = "term";
  private static final String LEFT_OPERAND = "left_operand";
  private static final String RIGHT_OPERAND = "right_operand";
  private static final String OPERAND = "operand";

  private static final String AND = "and";
  private static final String OR = "or";
  private static final String NOT = "not";
  private static final String TRUE = "true";
  private static final String FALSE = "false";

  private static final String UNBOUNDED_PREDICATE = "unbounded-predicate";
  private static final String NAMED = "named";

  private static final Set<Expression.Operation> oneInputs = Stream.of(Expression.Operation.IS_NULL,
          Expression.Operation.NOT_NULL,
          Expression.Operation.IS_NAN,
          Expression.Operation.NOT_NAN)
          .collect(Collectors.toSet());


  ExpressionParser() {
  }

  public static String toJson(Expression expression) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter(); //for testing purposes
      generator.writeStartObject(); //beginning of object
      toJson(expression, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write json"), e);
    }
  }

  public static void toJson(Expression expression, JsonGenerator generator) throws IOException {
    if (expression instanceof And) {
      toJson((And) expression, generator);
    } else if (expression instanceof Or) {
      toJson((Or) expression, generator);
    } else if (expression instanceof Not) {
      toJson((Not) expression, generator);
    } else if (expression instanceof True) {
      toJson((True) expression, generator);
    } else if (expression instanceof False) {
      toJson((False) expression, generator);
    } else if (expression instanceof Predicate) {
      toJson((Predicate) expression, generator);
    }
  }

  public static void toJson(And expression, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(AND);
    generator.writeObjectFieldStart(LEFT_OPERAND);
    ExpressionParser.toJson(expression.left(), generator);
    generator.writeObjectFieldStart(RIGHT_OPERAND);
    ExpressionParser.toJson(expression.right(), generator);
    generator.writeEndObject();
  }

  public static void toJson(Or expression, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(OR);
    generator.writeObjectFieldStart(LEFT_OPERAND);
    ExpressionParser.toJson(expression.left(), generator);
    generator.writeObjectFieldStart(RIGHT_OPERAND);
    ExpressionParser.toJson(expression.right(), generator);
    generator.writeEndObject();
  }

  public static void toJson(Not expression, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(NOT);
    generator.writeObjectFieldStart(OPERAND);
    ExpressionParser.toJson(expression.child(), generator);
    generator.writeEndObject();
  }

  public static void toJson(True expression, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(TRUE);
    generator.writeEndObject();
  }

  public static void toJson(False expression, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(FALSE);
    generator.writeEndObject();
  }

  public static void toJson(Predicate predicate, JsonGenerator generator) throws IOException {
    if (predicate instanceof UnboundPredicate) {
      toJson((UnboundPredicate) predicate, generator);
    } else if (predicate instanceof BoundLiteralPredicate) {
      toJson((BoundLiteralPredicate) predicate, generator); // Need to Implement
    } else if (predicate instanceof BoundSetPredicate) {
      toJson((BoundSetPredicate) predicate, generator); // Need to Implement
    } else if (predicate instanceof BoundUnaryPredicate) {
      toJson((BoundUnaryPredicate) predicate, generator); // Need to Implement
    }
  }

  public static void toJson(UnboundPredicate predicate, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(UNBOUNDED_PREDICATE);
    generator.writeFieldName(OPERATION); // check here for is null, not null, is nan, not null
    generator.writeString(predicate.op().toString().toLowerCase());
    generator.writeObjectFieldStart(TERM);
    ExpressionParser.toJson(predicate.term(), generator);
    if (!(oneInputs.contains(predicate.op()))){
      generator.writeObjectFieldStart(VALUE);
      ExpressionParser.toJson(predicate.literals(), generator);
    }

    generator.writeEndObject(); //closes predicate one
  }

  public static void toJson(Term term, JsonGenerator generator) throws IOException {
    if (term instanceof NamedReference) {
      toJson((NamedReference) term, generator);
    } else if (term instanceof BoundReference) {
      System.out.println("Not Implemented");
    }

    generator.writeEndObject(); // closes term one
  }

  public static void toJson(NamedReference term, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeString(NAMED);
    generator.writeFieldName(VALUE);
    generator.writeString(term.name());
    // closed in caller
  }

  public static void toJson(List<Literal> literals, JsonGenerator generator) throws IOException {
    generator.writeFieldName(TYPE);
    generator.writeStartArray();
    for (int i = 0; i < literals.size(); i++){
      toJsonType(literals.get(i), generator);
    }

    generator.writeEndArray();
    generator.writeFieldName(VALUE);
    generator.writeStartArray();
    for (int i = 0; i < literals.size(); i++){
      generator.writeString(literals.get(i).value().toString());
    }

    generator.writeEndArray();
    generator.writeEndObject(); //close literal
  }

  public static void toJsonType(Literal literal, JsonGenerator generator) throws IOException {
    if (literal instanceof Literals.AboveMax){
      generator.writeString("above-max");
    } else if (literal instanceof Literals.BelowMin){
      generator.writeString("below-min");
    } else if (literal instanceof Literals.BooleanLiteral){
      generator.writeString("boolean");
    } else if (literal instanceof Literals.IntegerLiteral){
      generator.writeString("integer");
    } else if (literal instanceof Literals.LongLiteral){
      generator.writeString("long");
    } else if (literal instanceof Literals.FloatLiteral){
      generator.writeString("float");
    } else if (literal instanceof Literals.DoubleLiteral){
      generator.writeString("double");
    } else if (literal instanceof Literals.DateLiteral){
      generator.writeString("date");
    } else if (literal instanceof Literals.TimeLiteral){
      generator.writeString("time");
    } else if (literal instanceof Literals.TimestampLiteral){
      generator.writeString("timestamp");
    } else if (literal instanceof Literals.DecimalLiteral){
      generator.writeString("decimal");
    } else if (literal instanceof Literals.StringLiteral){
      generator.writeString("string");
    } else if (literal instanceof Literals.UUIDLiteral){
      generator.writeString("uuid");
    } else if (literal instanceof Literals.FixedLiteral){
      generator.writeString("fixed");
    } else if (literal instanceof Literals.BinaryLiteral){
      generator.writeString("binary");
    }
  }
}
