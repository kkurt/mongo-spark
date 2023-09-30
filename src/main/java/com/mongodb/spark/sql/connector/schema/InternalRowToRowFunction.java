/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.spark.sql.connector.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.ExprId$;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;
import scala.jdk.CollectionConverters;

/**
 * An InternalRow to Row function that uses a resolved and bound encoder for the given schema.
 *
 * <p>A concrete {@code Function} implementation that is {@code Serializable}, so it can be
 * serialized and sent to executors.
 */
final class InternalRowToRowFunction implements Function<InternalRow, Row>, Serializable {
  private static final long serialVersionUID = 1L;

  private final ExpressionEncoder.Deserializer<Row> deserializer;

  @SuppressWarnings("unchecked")
  InternalRowToRowFunction(final StructType schema) {

    List<Attribute> attributesList = new ArrayList<>();
    for (StructField field : schema.fields()) {
      ExprId exprId = NamedExpression.newExprId();
      AttributeReference attributeReference = new AttributeReference(
          field.name(),
          field.dataType(),
          field.nullable(),
          field.metadata(),
          ExprId$.MODULE$.apply(exprId.id()),
          CollectionConverters.ListHasAsScala(new ArrayList<String>()).asScala().toSeq());
      attributesList.add(attributeReference);
    }

    Seq<Attribute> fields =
        CollectionConverters.ListHasAsScala(attributesList).asScala().toSeq();

    ExpressionEncoder<Row> rowEncoder2 = ExpressionEncoder.apply(schema);
    // AgnosticEncoder<Row> rowEncoder = RowEncoder$.MODULE$.encoderFor(schema).resolveAndBind();

    // this.deserializer = rowEncoder.resolveAndBind(attributeSeq,
    // SimpleAnalyzer$.MODULE$).createDeserializer();
    this.deserializer =
        rowEncoder2.resolveAndBind(fields, SimpleAnalyzer$.MODULE$).createDeserializer();
  }

  @Override
  public Row apply(final InternalRow internalRow) {
    return deserializer.apply(internalRow);
  }
}
