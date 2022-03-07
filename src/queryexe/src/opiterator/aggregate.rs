use super::{OpIterator, TupleIterator};
use common::{get_attr, AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::vec;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group.
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields.
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    /// Hashmap to make group-by columns to results from agg operations
    hm_groupby_aggops: HashMap<Vec<Field>, Vec<Vec<Field>>>,
}

impl Aggregator {
    /// Aggregator constructor.
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        let new_aggregator = Aggregator {
            agg_fields,
            groupby_fields,
            schema: schema.clone(),
            hm_groupby_aggops: HashMap::new(),
        };
        new_aggregator
    }

    /// Handles the creation of groups for aggregation.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        // extract the groupby_fields from the given tuple (these are primary key fields)
        let mut vec_primary_key = Vec::new();
        for i in self.groupby_fields.iter() {
            let primary_key_field = tuple.get_field(*i).unwrap().clone();
            vec_primary_key.push(primary_key_field);
        }
        // check if the vector of primary key fields already exists in hashmap
        if self.hm_groupby_aggops.contains_key(&vec_primary_key) {
            // extract the "value" (aka vec of vec<field>) associated with this groupby_fields key
            let mut vec_of_fields = self
                .hm_groupby_aggops
                .get(&vec_primary_key)
                .unwrap()
                .clone();

            for i in 0..self.agg_fields.len() {
                // extract existing computations vector for the agg_field with index i:
                let agg_field_computations = &mut vec_of_fields[i];
                let agg_field = self.agg_fields[i].clone();
                let agg_field_index = agg_field.field;

                let existing_count = &mut agg_field_computations[0].clone();
                let existing_sum = &mut agg_field_computations[1].clone();
                let existing_min = &mut agg_field_computations[2].clone();
                let existing_max = &mut agg_field_computations[3].clone();

                // extract the agg_field from tuple, given the index of the agg_field
                let tuple_agg_field = tuple.get_field(agg_field_index).unwrap();

                // check if agg_field is an int or string type
                match tuple_agg_field {
                    // if field is int type, then the initial 'sum' calculation is just the field value
                    Field::IntField(n) => {
                        let sum_val =
                            tuple_agg_field.unwrap_int_field() + existing_sum.unwrap_int_field();

                        let min_val = min(
                            Field::unwrap_int_field(&tuple_agg_field),
                            Field::unwrap_int_field(&existing_min),
                        );

                        let max_val = max(
                            Field::unwrap_int_field(&tuple_agg_field),
                            Field::unwrap_int_field(&existing_max),
                        );

                        // update the count value for the agg field
                        let count_val = Field::unwrap_int_field(&existing_count) + 1;

                        // update the contents of the agg_field_computations vector
                        agg_field_computations[0] = Field::IntField(count_val);
                        agg_field_computations[1] = Field::IntField(sum_val);
                        agg_field_computations[2] = Field::IntField(min_val);
                        agg_field_computations[3] = Field::IntField(max_val);

                        self.hm_groupby_aggops.get_mut(&vec_primary_key).unwrap()[i] =
                            agg_field_computations.to_vec();
                    }
                    // if field is string type, then 'sum' calculation is "None"
                    Field::StringField(n) => {
                        let sum_val = Field::unwrap_string_field(&tuple_agg_field);
                        let min_val = min(
                            Field::unwrap_string_field(&tuple_agg_field),
                            Field::unwrap_string_field(&existing_min),
                        );
                        let max_val = max(
                            Field::unwrap_string_field(&tuple_agg_field),
                            Field::unwrap_string_field(&existing_max),
                        );
                        // update the count value for the agg field
                        let count_val = Field::unwrap_int_field(&existing_count) + 1;

                        // update the contents of the agg_field_computations vector
                        agg_field_computations[0] = Field::IntField(count_val);
                        agg_field_computations[1] = Field::StringField(sum_val.to_string());
                        agg_field_computations[2] = Field::StringField(min_val.to_string());
                        agg_field_computations[3] = Field::StringField(max_val.to_string());
                        self.hm_groupby_aggops.get_mut(&vec_primary_key).unwrap()[i] =
                            agg_field_computations.to_vec();
                    }
                }
            }
        } else {
            // create new empty vector for new key-value pair to insert in hashmap
            let mut vec_of_fields = Vec::new();
            // iterate through the agg_fields and for each agg field get its corresponding
            // index to locate where it is in the row

            for mut i in 0..self.agg_fields.len() {
                let mut agg_field_computations = Vec::new();

                let agg_field = &self.agg_fields[i];
                let agg_field_index = agg_field.field;

                // extract the agg_field from tuple, given the index of the agg_field
                let tuple_agg_field = Tuple::get_field(tuple, agg_field_index).unwrap();

                let count_val = Field::IntField(1);
                let sum_val = tuple_agg_field;
                let min_val = tuple_agg_field;
                let max_val = tuple_agg_field;
                // push the newly created calculation fields into a vector

                agg_field_computations.push(count_val.clone());
                agg_field_computations.push(sum_val.clone());
                agg_field_computations.push(min_val.clone());
                agg_field_computations.push(max_val.clone());

                // push the newly created vector into overal vector

                vec_of_fields.push(agg_field_computations);
            }
            // push new key-value pair into hashmap. Key is the group-by field
            self.hm_groupby_aggops
                .insert(vec_primary_key, vec_of_fields);
        }
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        // extract the vec of agg_fields and corresponding agg_op
        let agg_fields = self.agg_fields.clone();
        // create an empty vector for vector of tuples that I will eventually iterate over
        let mut vec_to_iterate = Vec::new();
        for (key, val) in self.hm_groupby_aggops.iter() {
            // create an empty vector that will be filled with agg_op_values
            // (as fields) and later converted into a tuple
            let mut agg_op_values = Vec::new();
            for mut i in 0..agg_fields.len() {
                // extract the agg_field's index and aggregate operation to be applied on it
                let agg_field = agg_fields[i].clone();
                let agg_field_operation = agg_field.op;

                // extract the vector of agg_field calculations from hashmap:
                let vec_of_agg_field_calculations = val;
                // extract the vec of calculations for the "ith" agg_field:
                let calcs_for_ith_agg_field = vec_of_agg_field_calculations[i].clone();

                // find out which agg_op I want to conduct on this agg field and extract the
                // corresponding field from vec of agg_ops

                match agg_field_operation {
                    AggOp::Avg => {
                        // avg = sum/count, so calculate sum and count, then divide.
                        let agg_op_sum = Field::unwrap_int_field(&calcs_for_ith_agg_field[1]);
                        let agg_op_count = Field::unwrap_int_field(&calcs_for_ith_agg_field[0]);
                        let agg_op_value = agg_op_sum / agg_op_count;

                        // create field that carries this information
                        let agg_op_value_field = Field::IntField(agg_op_value);
                        // turn agg_op_value into a Field and push into vector of fields of agg_op_values
                        agg_op_values.push(agg_op_value_field);
                    }
                    AggOp::Count => {
                        // extract & unwrap the first field in vec to get "count" agg_op result
                        let agg_op_value = &calcs_for_ith_agg_field[0];
                        // turn agg_op_value into a Field and push into vector of fields of agg_op_values
                        agg_op_values.push(agg_op_value.clone());
                    }
                    AggOp::Max => {
                        // extract & unwrap the fourth field in vec to get "max" agg_op result
                        let agg_op_value = &calcs_for_ith_agg_field[3];
                        // turn agg_op_value into a Field and push into vector of fields of agg_op_values
                        agg_op_values.push(agg_op_value.clone());
                    }
                    AggOp::Min => {
                        // extract & unwrap the third field in vec to get "min" agg_op result
                        let agg_op_value = &calcs_for_ith_agg_field[2];
                        // turn agg_op_value into a Field and push into vector of fields of agg_op_values
                        agg_op_values.push(agg_op_value.clone());
                    }
                    AggOp::Sum => {
                        // extract & unwrap the second field in vec to get "sum" agg_op result
                        let agg_op_value = &calcs_for_ith_agg_field[1];

                        // turn agg_op_value into a Field and push into vector of fields of agg_op_values
                        agg_op_values.push(agg_op_value.clone());
                    }
                    _ => panic!("invalid aggregate operation!"),
                }
            }
            let primary_key_tuple = Tuple::new(key.to_vec());
            // convert vec of agg_ops (fields) into a tuple
            let agg_op_values_tuple = Tuple::new(agg_op_values);
            // merge both tuples
            let merged_tuple = Tuple::merge(&primary_key_tuple, &agg_op_values_tuple);
            // push tuple into final vector
            vec_to_iterate.push(merged_tuple);
        }
        // create the tuple-iterator and call it on the schema
        println!("Iterator: vec_to_iterate: {:?}", vec_to_iterate);
        let new_tupleiterator = TupleIterator::new(vec_to_iterate, self.schema.clone());
        return new_tupleiterator;
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Aggregator that Aggregate uses
    aggregator: Aggregator,
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
}

impl Aggregate {
    /// Aggregate constructor.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        // create new vec of AggregateFields
        let mut vec_AggFields = Vec::new();
        for i in 0..agg_indices.len() {
            let aggfield_index = agg_indices[i];
            let aggfield_op = ops[i];

            let new_AggField = AggregateField {
                field: aggfield_index,
                op: aggfield_op,
            };
            println!("Aggregate new() -- new_AggField index: {}", new_AggField.op);
            vec_AggFields.push(new_AggField);
        }

        // create a Vec<Attribute> that I can later use to create the schema

        let child_schema: TableSchema = child.get_schema().clone();
        let mut groupby_field_and_agg_field_attributes = Vec::new();

        // first push in the groupby_field attributes
        for i in 0..groupby_indices.len() {
            let groupby_index = groupby_indices[i];
            let groupby_name = groupby_names[i];

            let mut groupby_attribute = child_schema.get_attribute(groupby_index).unwrap().clone();
            groupby_attribute.name = groupby_name.to_string();

            groupby_field_and_agg_field_attributes.push(groupby_attribute);
        }

        // then push in the agg_field attributes
        for i in 0..agg_indices.len() {
            if agg_names[i] == "count" {
                let mut attr = Attribute::new("count".to_string(), DataType::Int);
                groupby_field_and_agg_field_attributes.push(attr);
            } else {
                let mut aggfield_attribute =
                    child_schema.get_attribute(agg_indices[i]).unwrap().clone();
                aggfield_attribute.name = agg_names[i].to_string();

                groupby_field_and_agg_field_attributes.push(aggfield_attribute);
            }
        }
        // now use the Vec<Attribute> to create the schema
        let new_schema = TableSchema::new(groupby_field_and_agg_field_attributes);

        // create aggregator
        let new_aggregator = Aggregator::new(
            vec_AggFields.clone(),
            groupby_indices.clone(),
            &child_schema,
        );
        let new_tuple_iterator = new_aggregator.iterator();
        // create aggregate
        let new_aggregate = Aggregate {
            aggregator: new_aggregator,
            groupby_fields: groupby_indices,
            agg_fields: vec_AggFields,
            agg_iter: Some(new_tuple_iterator),
            schema: new_schema,
            open: false,
            child,
        };
        new_aggregate
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.child.open().expect("agg_new");
        self.agg_iter.as_mut().unwrap().open();

        // call merge_tuple_into_group on every tuple produced by child.next()
        while let Some(t) = self.child.next()? {
            self.aggregator.merge_tuple_into_group(&t);
        }

        let new_tuple_iterator = self.aggregator.iterator();
        self.agg_iter = Some(new_tuple_iterator);
        self.agg_iter.as_mut().unwrap().open();

        self.open = true;

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        // gets next tuple
        if !self.open {
            return Err(CrustyError::IOError("Aggregate not".to_string()));
        } else {
            match self.agg_iter.as_mut() {
                Some(ti) => {
                    return ti.next();
                }
                None => {
                    return Err(CrustyError::IOError(
                        "Could not generate TupleIterator".to_string(),
                    ));
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if self.open == false {
            panic!("Aggregate is already closed!");
        } else {
            self.open = false;
        }

        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        //resets the state
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        self.child.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            println!("HERE IN test_merge_multiple_ops: 0");
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);
            println!("HERE IN test_merge_multiple_ops: 1");
            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );
            println!("HERE IN test_merge_multiple_ops: 2");

            let ti = tuples();
            for t in &ti {
                println!("HERE IN test_merge_multiple_ops -- on tuple: {:?}", t);
                agg.merge_tuple_into_group(t);
            }
            println!("HERE IN test_merge_multiple_ops: 3");
            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                println!("test_get_schema: attribute name -- {:?}", attr.name());
                println!(
                    "test_get_schema: expected attribute name -- {:?}",
                    expected_names[i]
                );
                assert_eq!(expected_names[i], attr.name());
                println!("test_get_schema: attribute dtype -- {:?}", *attr.dtype());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
