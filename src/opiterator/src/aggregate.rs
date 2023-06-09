#![allow(unused_imports)]
use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
//use serde::Serialize;
use std::cmp::{max, min};
use std::collections::HashMap;
//use common::Field::Null;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    hm: HashMap<Vec<Field>, Vec<Tuple>>,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        Self {
            agg_fields,
            groupby_fields,
            schema: schema.clone(),
            hm: HashMap::new(),
        }
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        //finds key for hashmap
        let mut k: Vec<Field> = Vec::new();
        for i in &self.groupby_fields {
            let f = tuple.clone().get_field(*i).unwrap().clone();
            k.push(f);
        }

        //inserts tuple into the hashmap
        if self.hm.contains_key(&k) {
            let mut v = self.hm.get(&k).unwrap().clone();
            v.push(tuple.clone());
            self.hm.insert(k, v);
        } else {
            let v = vec![tuple.clone()];
            self.hm.insert(k, v);
        }
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        let mut res: Vec<Tuple> = Vec::new();
        //loops though all values in hashmap
        for (k, val) in self.hm.iter() {
            let mut in_tup: Vec<Field> = Vec::new();
            for i in k {
                in_tup.push(i.clone());
            }
            //loops though all agg fields
            for agg_field in self.agg_fields.clone() {
                match agg_field.op {
                    //finds average of values in hashmap
                    AggOp::Avg => {
                        if val.is_empty() {
                            continue;
                        }
                        let test_val = val[0].get_field(agg_field.field).unwrap().clone();
                        let mut sum = 0;
                        let mut count = 0;
                        match test_val {
                            Field::IntField(_tv) => {
                                for tup in val.iter() {
                                    let cur_val = tup.get_field(agg_field.field).unwrap().clone();
                                    match cur_val {
                                        Field::IntField(v) => {
                                            sum += v;
                                            count += 1;
                                        }
                                        _ => {
                                            panic!("Cannot find average")
                                        }
                                    }
                                }
                            }
                            Field::StringField(_tv) => {
                                panic!("Cannot find average")
                            }
                            Field::Null => {
                                panic!("Cannot find average")
                            }
                        }
                        let avg = common::Field::IntField(sum / count);
                        in_tup.push(avg);
                    }
                    //finds count of values in hashmap
                    AggOp::Count => {
                        let len_tup = Field::IntField(val.len() as i32);
                        in_tup.push(len_tup);
                    }
                    //finds max of values in hashmap
                    AggOp::Max => {
                        if val.is_empty() {
                            continue;
                        }
                        let max_val = val[0].get_field(agg_field.field).unwrap().clone();
                        match max_val {
                            Field::IntField(_mv) => {
                                let mut a = max_val.clone();
                                for t in val.iter() {
                                    let cur_val = t.get_field(agg_field.field).unwrap().clone();
                                    a = max(cur_val, a);
                                }
                                in_tup.push(a);
                            }
                            Field::StringField(mv_str) => {
                                let mut m = common::Field::StringField(mv_str.clone());
                                for tup in val.iter() {
                                    let cur_val = tup.get_field(agg_field.field).unwrap().clone();
                                    if cur_val > m {
                                        m = cur_val;
                                    }
                                }
                                in_tup.push(m);
                            }
                            Field::Null => {
                                panic!("Cannot find max");
                            }
                        }
                    }
                    //finds min of values in hashmap
                    AggOp::Min => {
                        if val.is_empty() {
                            continue;
                        }
                        let min_val = val[0].get_field(agg_field.field).unwrap().clone();
                        match min_val {
                            Field::IntField(_mv) => {
                                let mut a = min_val.clone();
                                for t in val.iter() {
                                    let cur_val = t.get_field(agg_field.field).unwrap().clone();
                                    a = min(cur_val, a);
                                }
                                in_tup.push(a);
                            }
                            Field::StringField(mv_str) => {
                                let mut m = common::Field::StringField(mv_str.clone());
                                for tup in val.iter() {
                                    let cur_val = tup.get_field(agg_field.field).unwrap().clone();
                                    if cur_val < m {
                                        m = cur_val;
                                    }
                                }
                                in_tup.push(m);
                            }
                            Field::Null => {
                                panic!("Cannot find max");
                            }
                        }
                    }
                    //finds sum of values of hashmap
                    AggOp::Sum => {
                        if val.is_empty() {
                            continue;
                        }
                        let test_val = val[0].get_field(agg_field.field).unwrap().clone();
                        match test_val {
                            Field::IntField(_t) => {
                                let mut sum_vals = 0;
                                for tup in val.iter() {
                                    let cur_val = tup.get_field(agg_field.field).unwrap().clone();
                                    match cur_val {
                                        Field::IntField(v) => {
                                            sum_vals += v;
                                        }
                                        _ => {
                                            panic!("Cannot find average")
                                        }
                                    }
                                }
                                in_tup.push(common::Field::IntField(sum_vals));
                            }
                            Field::StringField(_t) => {
                                panic!("Cannot find sum")
                            }
                            Field::Null => {
                                panic!("Cannot find sum")
                            }
                        }
                    }
                }
            }
            //adds tuple (group by fields..results) to result
            let add_tup = Tuple::new(in_tup);
            res.push(add_tup);
        }

        let cur_schema = &self.schema;
        let s = cur_schema.clone();

        TupleIterator {
            tuples: res,
            schema: s,
            index: None,
        }
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
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
    aggregator: Option<Aggregator>,
    ind: usize,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        //finds list of aggregate fields
        let mut agg_fields: Vec<AggregateField> = Vec::new();
        for i in 0..agg_indices.len() {
            let agg_field = AggregateField {
                field: agg_indices[i],
                op: ops[i],
            };
            agg_fields.push(agg_field);
        }

        //creates schema which includes groupby names, agg names, and type
        let mut names: Vec<&str> = Vec::new();
        let mut dt: Vec<DataType> = Vec::new();
        for i in groupby_names {
            names.push(i);
        }

        for i in agg_names {
            names.push(i);
        }
        for _i in 0..names.len() {
            dt.push(DataType::Int);
        }
        let s = TableSchema::from_vecs(names, dt);

        //creates aggregate struct
        Self {
            groupby_fields: groupby_indices,
            agg_fields,
            agg_iter: None,
            schema: s,
            open: false,
            child,
            aggregator: None,
            ind: 0,
        }
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        match self.child.open() {
            Ok(()) => {}
            Err(_) => {
                return Err(CrustyError::CrustyError("Unable to rewind".to_string()));
            }
        }
        //creates aggregator and adds all tuples into it, and creates tuple iterator
        let mut agg = Aggregator::new(
            self.agg_fields.clone(),
            self.groupby_fields.clone(),
            &self.child.get_schema().clone(),
        );
        loop {
            let c = self.child.next().unwrap();
            match c {
                Some(c) => {
                    agg.merge_tuple_into_group(&c);
                }
                None => break,
            }
        }
        let ti = agg.iterator();

        //updates index, aggregator and tuple iterator
        self.ind = 0;
        self.aggregator = Some(agg);
        self.agg_iter = Some(ti);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }

        //finds correct tuple in tuple iterator to return as result
        match &self.agg_iter {
            Some(ai) => {
                let tups = &ai.tuples;
                if self.ind < tups.len() {
                    let a = &tups[self.ind];
                    self.ind += 1;
                    Ok(Some(a.clone()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        self.child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        self.child.rewind()?;
        self.close()?;
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
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

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

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

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
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
