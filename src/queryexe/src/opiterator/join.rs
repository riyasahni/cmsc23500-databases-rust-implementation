use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple};
use sqlparser::dialect::keywords::NO;
use std::collections::HashMap;

/// Compares the fields of two tuples using a predicate.
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
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        JoinPredicate {
            op: op,
            left_index: left_index,
            right_index: right_index,
        }
    }
}

/// Nested loop join implementation.
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    left_child: Box<dyn OpIterator>,
    left_index: usize,
    right_child: Box<dyn OpIterator>,
    right_index: usize,
    /// Schema of the result.
    schema: TableSchema,
    /// Bool to indicate open or closed Join
    is_open: bool,
    /// Bool keeps track of beginning of iteration
    is_beg: bool,
    /// save where left child is at
    lc_saved: Option<Tuple>,
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        // create the join predicate
        let join_predicate = JoinPredicate {
            op,
            left_index,
            right_index,
        };
        // create schema
        let right_schema = right_child.get_schema();
        let left_schema = left_child.get_schema();
        let new_schema = TableSchema::merge(&right_schema, &left_schema);
        // create new join struct
        Join {
            predicate: join_predicate,
            left_child,
            left_index,
            right_child,
            right_index,
            schema: new_schema,
            is_open: false,
            is_beg: true,
            lc_saved: Some(Tuple::new(Vec::new())),
        }
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        // open children
        self.right_child.open();
        self.left_child.open();
        // open join itself
        self.is_open = true;
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.is_open {
            panic!("Open OpIterator!")
        }
        let mut rt = self.right_child.next().unwrap();

        if self.is_beg {
            self.lc_saved = self.left_child.next().unwrap().clone();
            self.is_beg = false;
        }

        match rt {
            Some(r_tup) => {
                // Some right child next exists
                let l = self.lc_saved.clone();
                match l {
                    Some(l) => {
                        // check tuple's predicate and see if tuple should be joined or not
                        let r_field = Tuple::get_field(&r_tup, self.right_index).unwrap();
                        let l_field = Tuple::get_field(&l, self.left_index).unwrap();
                        let check = self.predicate.op.compare(&l_field, &r_field);

                        if check {
                            let tup = l.merge(&r_tup);
                            return Ok(Some(tup));
                        } else {
                            self.next()
                        }
                    }
                    None => return Ok(None),
                }
            }
            None => {
                self.right_child.rewind()?;
                rt = self.right_child.next().unwrap();
                let lt = self.left_child.next().unwrap();
                match lt {
                    Some(ref l) => {
                        // check tuple's predicate and see if tuple should be joined or not
                        let r_field =
                            Tuple::get_field(&rt.as_ref().unwrap(), self.right_index).unwrap();
                        let l_field = Tuple::get_field(&l, self.left_index).unwrap();
                        let check = self.predicate.op.compare(&l_field, &r_field);

                        if check {
                            let tup = l.merge(&rt.unwrap());
                            self.lc_saved = lt;
                            return Ok(Some(tup));
                        } else {
                            self.lc_saved = lt;
                            self.next()
                        }
                    }
                    None => {
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // check if already closed
        if !self.is_open {
            panic!("Already closed!");
        }
        // close children
        self.right_child.close()?;
        self.left_child.close()?;
        self.is_open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.is_open {
            panic!("Open the iterator!");
        }
        self.right_child.rewind()?;
        self.left_child.rewind()?;
        self.close()?;
        self.open()?;
        Ok(())
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation.
pub struct HashEqJoin {
    predicate: JoinPredicate,
    left_index: usize,
    right_index: usize,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    /// add element to store val of right child btw iterations
    saved_right: Option<Tuple>,
    right_stay: bool,
    schema: TableSchema,
    open: bool,
    /// need to store 1 child's info in hashmap
    join_hashmap: HashMap<Field, HashMap<usize, Tuple>>,
    /// keep track of explored values in hashmap
    hashmap_index: usize,
}
impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        let join_predicate = JoinPredicate::new(op, left_index, right_index);
        let left_schema = left_child.get_schema().clone();
        let right_schema = right_child.get_schema().clone();
        // schema is just left and right merged
        let joined_schema = left_schema.merge(&right_schema);
        let new_hasheq_join = HashEqJoin {
            predicate: join_predicate,
            left_index,
            right_index,
            left_child,
            right_child,
            saved_right: None,
            right_stay: false,
            schema: joined_schema,
            open: false,
            join_hashmap: HashMap::new(),
            hashmap_index: 0,
        };
        return new_hasheq_join;
    }
}
impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.left_child.open();
        self.right_child.open();
        self.open = true;
        // populate hash map for left child
        while let Some(l) = self.left_child.next().unwrap() {
            let key = l.get_field(self.left_index).unwrap();
            // case where key in outer hashmap
            if self.join_hashmap.contains_key(&key) {
                let inner_hashmap = self.join_hashmap.get_mut(&key).unwrap();
                let len_inner_hashmap = inner_hashmap.keys().len();
                inner_hashmap.insert((len_inner_hashmap - 1), l.clone());
            }
            // if not, add key, val pair to outer hashmap
            else {
                let mut new_inner_hashmap = HashMap::new();
                new_inner_hashmap.insert(0, l.clone());
                self.join_hashmap.insert(key.clone(), new_inner_hashmap);
            }
        }
        Ok(())
    }
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        // set right as either stored or next
        let mut right_val = self.saved_right.clone();
        let mut right_val_new = Tuple::new(Vec::new());
        if !self.right_stay {
            self.right_stay = true;
            right_val = self.right_child.next().unwrap();
            match right_val {
                Some(x) => right_val_new = x,
                None => return Ok(None),
            };
            self.saved_right = Some(right_val_new.clone());
        }
        /// search hashmap for matches, use hashmap index to deal with multiple values / key
        let right_field = right_val_new.get_field(self.right_index);
        match right_field {
            Some(rf) => {
                if self.join_hashmap.contains_key(&rf) {
                    let inner_hashmap = self.join_hashmap[&rf].clone();
                    let left_val = &inner_hashmap[&self.hashmap_index];
                    let merged_val = left_val.merge(&right_val_new);
                    self.hashmap_index += 1;
                    if self.hashmap_index == inner_hashmap.keys().len() {
                        self.hashmap_index = 0;
                        self.right_stay = false;
                    } else {
                        self.hashmap_index += 1;
                        self.right_stay = true;
                    }
                    return Ok(Some(merged_val));
                } else {
                    self.hashmap_index = 0;
                    self.right_stay = false;
                    return self.next();
                }
            }
            None => return Ok(None),
        }
    }
    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }
    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.right_stay = false;
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
