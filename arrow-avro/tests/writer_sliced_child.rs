// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Regression tests ensuring the Avro writer's list/map encoders index the
//! value/key child by the logical (0-based) offset rather than double-counting
//! the child array's own `offset()`.

use arrow_array::*;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::AvroWriter;
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields, Schema};
use std::io::Cursor;
use std::sync::Arc;

#[test]
fn list_of_bool_with_sliced_child_roundtrips() {
    let full = BooleanArray::from(vec![true, true, false, true, false, true]);
    let child = full.slice(2, 4); // logical [false, true, false, true], offset()==2
    assert_eq!(child.offset(), 2);
    let offsets = OffsetBuffer::new(vec![0i32, 4].into());
    let item_field = Arc::new(Field::new("item", DataType::Boolean, true));
    let list = ListArray::new(
        item_field.clone(),
        offsets,
        Arc::new(child) as ArrayRef,
        None,
    );
    let schema = Schema::new(vec![Field::new("l", DataType::List(item_field), false)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(list) as ArrayRef]).unwrap();
    let mut w = AvroWriter::new(Vec::<u8>::new(), schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    let bytes = w.into_inner();
    let mut r = ReaderBuilder::new().build(Cursor::new(bytes)).unwrap();
    let out = r.next().unwrap().unwrap();
    let out_list = out
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let elem = out_list.value(0);
    let eb = elem.as_any().downcast_ref::<BooleanArray>().unwrap();
    let got: Vec<bool> = (0..eb.len()).map(|i| eb.value(i)).collect();
    assert_eq!(got, vec![false, true, false, true]);
}

#[test]
fn map_with_sliced_bool_value_child_roundtrips() {
    // Boolean-valued map whose value child has a non-zero offset().
    let keys = StringArray::from(vec!["a", "b", "c", "d"]);
    let values_full = BooleanArray::from(vec![true, false, true, false, true]);
    let values = values_full.slice(1, 4); // logical [false, true, false, true], offset()==1
    assert_eq!(values.offset(), 1);

    let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
    let value_field = Arc::new(Field::new("value", DataType::Boolean, true));
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            key_field.as_ref().clone(),
            value_field.as_ref().clone(),
        ])),
        false,
    ));
    let entries = StructArray::new(
        Fields::from(vec![key_field.as_ref().clone(), value_field.as_ref().clone()]),
        vec![
            Arc::new(keys) as ArrayRef,
            Arc::new(values) as ArrayRef,
        ],
        None,
    );
    let offsets = OffsetBuffer::new(vec![0i32, 4].into());
    let map = MapArray::new(entries_field.clone(), offsets, entries, None, false);
    let schema = Schema::new(vec![Field::new(
        "m",
        DataType::Map(entries_field, false),
        false,
    )]);
    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(map) as ArrayRef]).unwrap();
    let mut w = AvroWriter::new(Vec::<u8>::new(), schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    let bytes = w.into_inner();
    let mut r = ReaderBuilder::new().build(Cursor::new(bytes)).unwrap();
    let out = r.next().unwrap().unwrap();
    let out_map = out.column(0).as_any().downcast_ref::<MapArray>().unwrap();
    let entry = out_map.value(0);
    let out_keys = entry
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let out_vals = entry
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let mut pairs: Vec<(String, bool)> = (0..entry.len())
        .map(|i| (out_keys.value(i).to_string(), out_vals.value(i)))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        pairs,
        vec![
            ("a".to_string(), false),
            ("b".to_string(), true),
            ("c".to_string(), false),
            ("d".to_string(), true),
        ]
    );
}
