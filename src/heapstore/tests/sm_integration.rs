extern crate common;
extern crate heapstore as sm;
use std::ptr::eq;

use common::ids::Permissions;
use common::ids::{ContainerId, TransactionId};
use common::storage_trait::StorageTrait;
use common::testutil::*;
use rand::{thread_rng, Rng};
use sm::storage_manager::StorageManager;
use std::collections::VecDeque;

const RO: Permissions = Permissions::ReadOnly;

#[test]
fn sm_inserts() {
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let num_vals: Vec<usize> = vec![10, 50, 75, 100, 500, 1000];
    for i in num_vals {
        let vals1 = get_random_vec_of_byte_vec(i, 50, 100);
        let cid = i as ContainerId;
        sm.create_table(cid).unwrap();
        sm.insert_values(cid, vals1.clone(), t);
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).collect();
        assert!(
            compare_unordered_byte_vecs(&vals1, check_vals),
            "Insert of size {} should be equal",
            i
        );
    }
}

/*#[test]
fn sm_insert_delete() {
    let mut rng = thread_rng();
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let mut vals1 = get_random_vec_of_byte_vec(75, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let mut val_ids = sm.insert_values(cid, vals1.clone(), t);
    //println!("val_ids before delete: {:?}", val_ids);
    for _ in 0..10 {
        let idx_to_del = rng.gen_range(0..vals1.len());
        println!("index to delete: {}", idx_to_del);
        //   println!("vals1 (before delete value): {:?}", &vals1);
        sm.delete_value(val_ids[idx_to_del], t).unwrap();
        // println!("val_ids after delete value: {:?}", val_ids);
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).collect();
        // println!("vals1 (before swap_remove): {:?}", &vals1);
        /*println!(
            "check (after delete value & before swap_remove): {:?}",
            check_vals.clone()
        );*/
        assert!(!compare_unordered_byte_vecs(&vals1, check_vals.clone()));
        vals1.swap_remove(idx_to_del);
        val_ids.swap_remove(idx_to_del);
        println!("vals1 (after swap_remove): {:?}", &vals1);
        println!("");
        println!("");
        println!("check (after swap_remove): {:?}", check_vals.clone());
        assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    }
}*/
#[test]
fn sm_insert_delete() {
    let mut rng = thread_rng();
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let mut vals1 = get_random_vec_of_byte_vec(75, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let mut val_ids = sm.insert_values(cid, vals1.clone(), t);
    for _ in 0..10 {
        let idx_to_del = rng.gen_range(0..vals1.len());
        println!("in sm_integration idx_to_del: {}", idx_to_del);
        sm.delete_value(val_ids[idx_to_del], t).unwrap();
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).collect();
        assert!(!compare_unordered_byte_vecs(&vals1, check_vals.clone()));
        vals1.swap_remove(idx_to_del);
        val_ids.swap_remove(idx_to_del);
        println!("in sm_integration vals1: {:?}", vals1);
        println!("");
        println!("");
        println!("in sm_integration check_vals: {:?}", check_vals);
        assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    }
}

#[test]
fn sm_insert_updates() {
    let mut rng = thread_rng();
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let mut vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let mut val_ids = sm.insert_values(cid, vals1.clone(), t);
    for _ in 0..10 {
        let idx_to_upd = rng.gen_range(0..vals1.len());
        let new_bytes = get_random_byte_vec(15);
        let new_val_id = sm
            .update_value(new_bytes.clone(), val_ids[idx_to_upd], t)
            .unwrap();
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).collect();
        assert!(!compare_unordered_byte_vecs(&vals1, check_vals.clone()));
        vals1[idx_to_upd] = new_bytes;
        val_ids[idx_to_upd] = new_val_id;
        assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    }
}

#[test]
#[should_panic]
fn sm_no_container() {
    //   println!("inside sm_no_container");
    let sm = StorageManager::new_test_sm();
    //   println!("called new_test_sm");
    let t = TransactionId::new();
    //   println!("called new");
    let vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    sm.insert_values(1, vals1, t);
}

#[test]
fn sm_test_shutdown() {
    //  println!("SM TEST SHUTDOWN: I made it here in shutdown-9999");
    let path = String::from("tmp");
    let sm = StorageManager::new(path.clone());
    //  println!("SM TEST SHUTDOWN: after creating new SM");
    let t = TransactionId::new();

    let vals1 = get_random_vec_of_byte_vec(2, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let _val_ids = sm.insert_values(cid, vals1.clone(), t);
    //  println!("SM TEST SHUTDOWN: I made it here before shutdown");
    sm.shutdown();
    //  println!("SM TEST SHUTDOWN: I made it here after shutdown");
    let sm2 = StorageManager::new(path);
    //  println!("SM TEST SHUTDOWN: I made it here after shutdown");
    let check_vals: Vec<Vec<u8>> = sm2.get_iterator(cid, t, RO).collect();
    //   println!("SM TEST SHUTDOWN: I made it here get_iteraor");
    println!("in sm_integration vals1: {:?}", vals1);
    println!("");
    println!("");
    println!("in sm_integration check_vals: {:?}", check_vals);
    assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    sm2.reset().unwrap();
}
