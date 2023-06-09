#![allow(unused_imports)]
use super::OpIterator;
use crate::opiterator::TupleIterator;
use common::{CrustyError, SimplePredicateOp, TableSchema, Tuple};
//use std::collections::HashMap;

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        Self {
            op,
            left_index,
            right_index,
        }
    }
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    cur_left_child: Option<Tuple>,
    /// Schema of the result.
    schema: TableSchema,
    open: bool,
    beginning: bool,
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            predicate: JoinPredicate::new(op, left_index, right_index),
            schema: left_child
                .get_schema()
                .clone()
                .merge(&right_child.get_schema().clone()),
            left_child,
            right_child,
            cur_left_child: None,
            open: false,
            beginning: true,
        }
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.left_child.open()?;
        self.right_child.open()?;
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        let r = self.right_child.next().unwrap();
        match r {
            Some(r) => {
                //if first time calling next, set cur left child as left child.next
                if self.beginning {
                    self.beginning = false;
                    self.cur_left_child = self.left_child.next().unwrap();
                }
                //matches current left child
                let l = self.cur_left_child.clone();
                match l {
                    None => Ok(None),
                    Some(l) => {
                        //checks if predicate is satistied between left and right. if so, merges
                        let left_field = l.get_field(self.predicate.left_index).unwrap();
                        let right_field = r.get_field(self.predicate.right_index).unwrap();
                        if self.predicate.op.compare(left_field, right_field) {
                            Ok(Some(l.merge(&r)))
                        } else {
                            self.next()
                        }
                    }
                }
            }
            None => {
                //if reach end of rigth child, call left child.next
                let l = self.left_child.next().unwrap();
                self.cur_left_child = l.clone();
                match l {
                    Some(l) => {
                        //rewinds right child and checks if left and right satify predicate
                        match self.right_child.rewind() {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(CrustyError::CrustyError(
                                    "Unable to rewind".to_string(),
                                ));
                            }
                        }
                        let r = self.right_child.next().unwrap().unwrap();
                        let left_field = l.get_field(self.predicate.left_index).unwrap();
                        let right_field = r.get_field(self.predicate.right_index).unwrap();
                        if self.predicate.op.compare(left_field, right_field) {
                            Ok(Some(l.merge(&r)))
                        } else {
                            self.next()
                        }
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.beginning = true;
        self.close()?;
        self.open()
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,

    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,

    schema: TableSchema,
    open: bool,
    beginning: bool,
    cur_left_child: Option<Tuple>,
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            predicate: JoinPredicate::new(op, left_index, right_index),
            schema: left_child
                .get_schema()
                .clone()
                .merge(&right_child.get_schema().clone()),
            left_child,
            right_child,
            open: false,
            beginning: true,
            cur_left_child: None,
        }
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;

        match self.right_child.open() {
            Ok(()) => {}
            Err(_) => {
                return Err(CrustyError::CrustyError(
                    "Unable to Open Right Child".to_string(),
                ));
            }
        }
        match self.left_child.open() {
            Ok(()) => {}
            Err(_) => {
                return Err(CrustyError::CrustyError(
                    "Unable to Open Left Child".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        let r = self.right_child.next().unwrap();
        match r {
            Some(r) => {
                //if first time calling next, set cur left child as left child.next
                if self.beginning {
                    self.beginning = false;
                    self.cur_left_child = self.left_child.next().unwrap();
                }
                //matches current left child
                let l = self.cur_left_child.clone();
                match l {
                    None => Ok(None),
                    Some(l) => {
                        //checks if predicate is satistied between left and right. if so, merges
                        let left_field = l.get_field(self.predicate.left_index).unwrap();
                        let right_field = r.get_field(self.predicate.right_index).unwrap();
                        if self.predicate.op.compare(left_field, right_field) {
                            Ok(Some(l.merge(&r)))
                        } else {
                            self.next()
                        }
                    }
                }
            }
            None => {
                //if reach end of rigth child, call left child.next
                let l = self.left_child.next().unwrap();
                self.cur_left_child = l.clone();
                match l {
                    Some(l) => {
                        //rewinds right child and checks if left and right satify predicate
                        match self.right_child.rewind() {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(CrustyError::CrustyError(
                                    "Unable to rewind".to_string(),
                                ));
                            }
                        }
                        let r = self.right_child.next().unwrap().unwrap();
                        let left_field = l.get_field(self.predicate.left_index).unwrap();
                        let right_field = r.get_field(self.predicate.right_index).unwrap();
                        if self.predicate.op.compare(left_field, right_field) {
                            Ok(Some(l.merge(&r)))
                        } else {
                            self.next()
                        }
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError("Operator not Opened".to_string()));
        }

        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.beginning = true;
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
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6],
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;

        let mut eq_join = eq_join();
        eq_join.open()?;

        let acutal = op.next()?;
        let expected = eq_join.next()?;

        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }
}
