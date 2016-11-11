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
//  val intFieldType = new FieldType()
//  intFieldType.setTokenized(true);
//  intFieldType.setOmitNorms(true);
//  intFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
//  intFieldType.setNumericType(FieldType.NumericType.INT);
//  intFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
//  intFieldType.setStored(true);
//  intFieldType.freeze();
//
//  val longFieldType = new FieldType()
//  longFieldType.setTokenized(true);
//  longFieldType.setOmitNorms(true)
//  longFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
//  longFieldType.setNumericType(FieldType.NumericType.LONG);
//  longFieldType.setStored(true);
//  longFieldType.freeze();
//
//
//  val doubleFieldType = new FieldType()
//  doubleFieldType.setTokenized(true);
//  doubleFieldType.setOmitNorms(true);
//  doubleFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
//  doubleFieldType.setNumericType(FieldType.NumericType.DOUBLE);
//  doubleFieldType.setStored(true);
//  doubleFieldType.freeze();
//
//  val floatFieldType = new FieldType()
//  floatFieldType.setTokenized(true);
//  floatFieldType.setOmitNorms(true);
//  floatFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
//  floatFieldType.setNumericType(FieldType.NumericType.FLOAT);
//  floatFieldType.setNumericPrecisionStep(NumericUtils.PRECISION_STEP_DEFAULT_32);
//  floatFieldType.setStored(true);
//  floatFieldType.freeze();
//
//  val textFieldType = new FieldType()
//  textFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
//  textFieldType.setTokenized(true);
//  textFieldType.setStored(true);
//  textFieldType.freeze();

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
