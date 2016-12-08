/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.index.lucenerdd

import org.apache.lucene.document.FieldType
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.util.NumericUtils

object AllFields {
  // Index but no store
  val noStore_intFieldType = new FieldType()
  noStore_intFieldType.setTokenized(true);
  noStore_intFieldType.setOmitNorms(true);
  noStore_intFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
  noStore_intFieldType.setNumericType(FieldType.NumericType.INT);
  noStore_intFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
  noStore_intFieldType.setStored(false);
  noStore_intFieldType.freeze();

  val noStore_longFieldType = new FieldType()
  noStore_longFieldType.setTokenized(true);
  noStore_longFieldType.setOmitNorms(true)
  noStore_longFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
  noStore_longFieldType.setNumericType(FieldType.NumericType.LONG);
  noStore_longFieldType.setStored(false);
  noStore_longFieldType.freeze();


  val noStore_doubleFieldType = new FieldType()
  noStore_doubleFieldType.setTokenized(true);
  noStore_doubleFieldType.setOmitNorms(true);
  noStore_doubleFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
  noStore_doubleFieldType.setNumericType(FieldType.NumericType.DOUBLE);
  noStore_doubleFieldType.setStored(false);
  noStore_doubleFieldType.freeze();

  val noStore_floatFieldType = new FieldType()
  noStore_floatFieldType.setTokenized(true);
  noStore_floatFieldType.setOmitNorms(true);
  noStore_floatFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
  noStore_floatFieldType.setNumericType(FieldType.NumericType.FLOAT);
  noStore_floatFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
  noStore_floatFieldType.setStored(false);
  noStore_floatFieldType.freeze();

  val noStore_textFieldType = new FieldType()
  noStore_textFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
  noStore_textFieldType.setTokenized(true);
  noStore_textFieldType.setStored(false);
  noStore_textFieldType.freeze();

  // No Store and no index

  val no_intFieldType = new FieldType()
  no_intFieldType.setTokenized(false);
  no_intFieldType.setOmitNorms(true);
  no_intFieldType.setIndexOptions(IndexOptions.NONE);
  no_intFieldType.setNumericType(FieldType.NumericType.INT);
  no_intFieldType.setStored(true)
  no_intFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
  no_intFieldType.freeze();

  val no_longFieldType = new FieldType()
  no_longFieldType.setTokenized(false)
  no_longFieldType.setIndexOptions(IndexOptions.NONE)
  no_longFieldType.setNumericType(FieldType.NumericType.LONG);
  no_longFieldType.setStored(true)
  no_longFieldType.freeze()

  val no_doubleFieldType = new FieldType()
  no_doubleFieldType.setTokenized(false);
  no_doubleFieldType.setOmitNorms(true);
  no_doubleFieldType.setIndexOptions(IndexOptions.NONE);
  no_doubleFieldType.setNumericType(FieldType.NumericType.DOUBLE);
  no_doubleFieldType.setStored(true)
  no_doubleFieldType.freeze()

  val no_floatFieldType = new FieldType()
  no_floatFieldType.setTokenized(false);
  no_floatFieldType.setOmitNorms(true);
  no_floatFieldType.setIndexOptions(IndexOptions.NONE);
  no_floatFieldType.setNumericType(FieldType.NumericType.FLOAT);
  no_floatFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
  no_floatFieldType.setStored(true)
  no_floatFieldType.freeze();

  val no_textFieldType = new FieldType()
  no_textFieldType.setTokenized(false);
  no_textFieldType.setIndexOptions(IndexOptions.NONE);
  no_textFieldType.setStored(true)
  no_textFieldType.freeze();

  // Store but no index

  val notIndex_intFieldType = new FieldType()
  notIndex_intFieldType.setTokenized(false);
  notIndex_intFieldType.setOmitNorms(true);
  notIndex_intFieldType.setIndexOptions(IndexOptions.NONE);
  notIndex_intFieldType.setNumericType(FieldType.NumericType.INT);
  notIndex_intFieldType.setStored(true)
  notIndex_intFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
  notIndex_intFieldType.freeze();

  val notIndex_longFieldType = new FieldType()
  notIndex_longFieldType.setTokenized(false)
  notIndex_longFieldType.setIndexOptions(IndexOptions.NONE)
  notIndex_longFieldType.setNumericType(FieldType.NumericType.LONG);
  notIndex_longFieldType.setStored(true)
  notIndex_longFieldType.freeze()

  val notIndex_doubleFieldType = new FieldType()
  notIndex_doubleFieldType.setTokenized(false);
  notIndex_doubleFieldType.setOmitNorms(true);
  notIndex_doubleFieldType.setIndexOptions(IndexOptions.NONE);
  notIndex_doubleFieldType.setNumericType(FieldType.NumericType.DOUBLE);
  notIndex_doubleFieldType.setStored(true)
  notIndex_doubleFieldType.freeze()

  val notIndex_floatFieldType = new FieldType()
  notIndex_floatFieldType.setTokenized(false);
  notIndex_floatFieldType.setOmitNorms(true);
  notIndex_floatFieldType.setIndexOptions(IndexOptions.NONE);
  notIndex_floatFieldType.setNumericType(FieldType.NumericType.FLOAT);
  notIndex_floatFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
  notIndex_floatFieldType.setStored(true)
  notIndex_floatFieldType.freeze();

  val notIndex_textFieldType = new FieldType()
  notIndex_textFieldType.setTokenized(false);
  notIndex_textFieldType.setIndexOptions(IndexOptions.NONE);
  notIndex_textFieldType.setStored(true)
  notIndex_textFieldType.freeze();
}
