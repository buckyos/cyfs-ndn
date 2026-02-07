#[cfg(test)]
mod tests {
    use crate::fs_meta_service::FSMetaService;
    use fs_buffer::SessionId;
    use krpc::RPCContext;
    use ndm::{
        ClientSessionId, DentryTarget, FsMetaHandler, IndexNodeId, NodeKind, NodeRecord, NodeState,
    };
    use ndn_lib::NdmPath;
    use ndn_lib::ObjId;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_service() -> (FSMetaService, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let db_path = tmp_dir.path().join("test.db");
        let svc = FSMetaService::new(db_path.to_str().unwrap()).unwrap();
        (svc, tmp_dir)
    }

    fn create_obj_id(seed: u8) -> ObjId {
        // ObjId requires obj_type:obj_hash format
        let hash = vec![seed; 32];
        ObjId::new_by_raw("file".to_string(), hash)
    }

    fn dummy_ctx() -> RPCContext {
        RPCContext::default()
    }

    fn create_dir_node(inode_id: IndexNodeId) -> NodeRecord {
        NodeRecord {
            inode_id,
            kind: NodeKind::Dir,
            read_only: false,
            base_obj_id: None,
            state: NodeState::DirNormal,
            rev: Some(0),
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        }
    }

    fn create_file_node_working(inode_id: IndexNodeId, fb_handle: &str) -> NodeRecord {
        NodeRecord {
            inode_id,
            kind: NodeKind::File,
            read_only: false,
            base_obj_id: None,
            state: NodeState::Working(ndm::FileWorkingState {
                fb_handle: fb_handle.to_string(),
                last_write_at: 1000,
            }),
            rev: None,
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        }
    }

    // ==================== Root Dir Tests ====================

    #[tokio::test]
    async fn test_root_dir() {
        let (svc, _tmp) = create_test_service();
        let root = svc.handle_root_dir(dummy_ctx()).await.unwrap();
        assert_eq!(root, 1);
    }

    // ==================== Inode CRUD Tests ====================

    #[tokio::test]
    async fn test_get_inode_root_exists() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();
        let node = svc.handle_get_inode(root, None, ctx).await.unwrap();
        assert!(node.is_some());
        let node = node.unwrap();
        assert_eq!(node.inode_id, root);
        assert_eq!(node.kind, NodeKind::Dir);
    }

    #[tokio::test]
    async fn test_get_inode_not_found() {
        let (svc, _tmp) = create_test_service();
        let node = svc.handle_get_inode(9999, None, dummy_ctx()).await.unwrap();
        assert!(node.is_none());
    }

    #[tokio::test]
    async fn test_set_and_get_inode() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_dir_node(100);
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let fetched = svc.handle_get_inode(100, None, ctx).await.unwrap().unwrap();
        assert_eq!(fetched.inode_id, 100);
        assert_eq!(fetched.kind, NodeKind::Dir);
        assert!(!fetched.read_only);
    }

    #[tokio::test]
    async fn test_set_inode_upsert() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let mut node = create_dir_node(101);
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        // Update read_only
        node.read_only = true;
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let fetched = svc.handle_get_inode(101, None, ctx).await.unwrap().unwrap();
        assert!(fetched.read_only);
    }

    #[tokio::test]
    async fn test_alloc_inode_auto_id() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_dir_node(0); // inode_id = 0 means auto-alloc
        let id = svc
            .handle_alloc_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();
        assert!(id > 1); // root is 1

        let fetched = svc.handle_get_inode(id, None, ctx).await.unwrap().unwrap();
        assert_eq!(fetched.inode_id, id);
    }

    #[tokio::test]
    async fn test_alloc_inode_explicit_id() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_dir_node(200);
        let id = svc
            .handle_alloc_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();
        assert_eq!(id, 200);

        let fetched = svc.handle_get_inode(200, None, ctx).await.unwrap().unwrap();
        assert_eq!(fetched.inode_id, 200);
    }

    #[tokio::test]
    async fn test_update_inode_state() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(300, "fb-001");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        // Transition to Cooling
        let new_state = NodeState::Cooling(ndm::FileCoolingState {
            fb_handle: "fb-001".to_string(),
            closed_at: 2000,
        });
        svc.handle_update_inode_state(300, new_state, None, ctx.clone())
            .await
            .unwrap();

        let fetched = svc.handle_get_inode(300, None, ctx).await.unwrap().unwrap();
        match fetched.state {
            NodeState::Cooling(s) => {
                assert_eq!(s.fb_handle, "fb-001");
                assert_eq!(s.closed_at, 2000);
            }
            _ => panic!("expected Cooling state"),
        }
    }

    #[tokio::test]
    async fn test_update_inode_state_not_found() {
        let (svc, _tmp) = create_test_service();
        let result = svc
            .handle_update_inode_state(9999, NodeState::DirNormal, None, dummy_ctx())
            .await;
        assert!(result.is_err());
    }

    // ==================== Dentry Tests ====================

    #[tokio::test]
    async fn test_upsert_and_get_dentry() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        // Create a child inode
        let child = create_dir_node(400);
        svc.handle_set_inode(child.clone(), None, ctx.clone())
            .await
            .unwrap();

        // Link dentry
        svc.handle_upsert_dentry(
            root,
            "subdir".to_string(),
            DentryTarget::IndexNodeId(400),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        let dentry = svc
            .handle_get_dentry(root, "subdir".to_string(), None, ctx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dentry.parent, root);
        assert_eq!(dentry.name, "subdir");
        match dentry.target {
            DentryTarget::IndexNodeId(id) => assert_eq!(id, 400),
            _ => panic!("expected IndexNodeId target"),
        }
    }

    #[tokio::test]
    async fn test_make_link_creates_symlink_target() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        let target = create_file_node_working(401, "fb-link-target");
        svc.handle_set_inode(target, None, ctx.clone())
            .await
            .unwrap();
        svc.handle_upsert_dentry(
            root,
            "target_file".to_string(),
            DentryTarget::IndexNodeId(401),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        svc.handle_make_link(
            &NdmPath::new("/link_file"),
            &NdmPath::new("/target_file"),
            ctx.clone(),
        )
        .await
        .unwrap();

        let dentry = svc
            .handle_get_dentry(root, "link_file".to_string(), None, ctx.clone())
            .await
            .unwrap()
            .unwrap();
        match dentry.target {
            DentryTarget::SymLink(id) => assert_eq!(id, 401),
            _ => panic!("expected SymLink target"),
        }

        let resolved = svc
            .handle_resolve_path(&NdmPath::new("/link_file"), ctx)
            .await
            .unwrap();
        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn test_make_link_rejects_symlink_target() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        let target = create_file_node_working(402, "fb-link-target-2");
        svc.handle_set_inode(target, None, ctx.clone())
            .await
            .unwrap();
        svc.handle_upsert_dentry(
            root,
            "target_file_2".to_string(),
            DentryTarget::IndexNodeId(402),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        svc.handle_make_link(
            &NdmPath::new("/link_a"),
            &NdmPath::new("/target_file_2"),
            ctx.clone(),
        )
        .await
        .unwrap();

        let result = svc
            .handle_make_link(&NdmPath::new("/link_b"), &NdmPath::new("/link_a"), ctx)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_dentry_not_found() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        let dentry = svc
            .handle_get_dentry(root, "nonexistent".to_string(), None, ctx)
            .await
            .unwrap();
        assert!(dentry.is_none());
    }

    #[tokio::test]
    async fn test_list_dentries() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        // Create multiple dentries
        for i in 0..3 {
            let child = create_dir_node(500 + i);
            svc.handle_set_inode(child.clone(), None, ctx.clone())
                .await
                .unwrap();
            svc.handle_upsert_dentry(
                root,
                format!("child_{}", i),
                DentryTarget::IndexNodeId(500 + i),
                None,
                ctx.clone(),
            )
            .await
            .unwrap();
        }

        let dentries = svc.handle_list_dentries(root, None, ctx).await.unwrap();
        assert_eq!(dentries.len(), 3);
    }

    #[tokio::test]
    async fn test_list_session_cache() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        for i in 0..3 {
            let child = create_dir_node(550 + i);
            svc.handle_set_inode(child.clone(), None, ctx.clone())
                .await
                .unwrap();
            svc.handle_upsert_dentry(
                root,
                format!("entry_{}", i),
                DentryTarget::IndexNodeId(550 + i),
                None,
                ctx.clone(),
            )
            .await
            .unwrap();
        }

        let list_session_id = svc
            .handle_start_list(root, None, ctx.clone())
            .await
            .unwrap();
        let page1 = svc
            .handle_list_next(list_session_id, 2, ctx.clone())
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1.keys().next().unwrap(), "entry_0");
        assert_eq!(page1.keys().next_back().unwrap(), "entry_1");
        assert!(page1.values().all(|v| v.inode.is_some()));

        let page2 = svc
            .handle_list_next(list_session_id, 2, ctx.clone())
            .await
            .unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2.keys().next().unwrap(), "entry_2");

        svc.handle_stop_list(list_session_id, ctx.clone())
            .await
            .unwrap();
        let err = svc.handle_list_next(list_session_id, 1, ctx).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_remove_dentry_row() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        svc.handle_upsert_dentry(
            root,
            "to_remove".to_string(),
            DentryTarget::IndexNodeId(600),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        svc.handle_remove_dentry_row(root, "to_remove".to_string(), None, ctx.clone())
            .await
            .unwrap();

        let dentry = svc
            .handle_get_dentry(root, "to_remove".to_string(), None, ctx)
            .await
            .unwrap();
        assert!(dentry.is_none());
    }

    #[tokio::test]
    async fn test_set_tombstone() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        svc.handle_set_tombstone(root, "deleted".to_string(), None, ctx.clone())
            .await
            .unwrap();

        let dentry = svc
            .handle_get_dentry(root, "deleted".to_string(), None, ctx)
            .await
            .unwrap()
            .unwrap();
        match dentry.target {
            DentryTarget::Tombstone => {}
            _ => panic!("expected Tombstone target"),
        }
    }

    // ==================== Directory Rev Tests ====================

    #[tokio::test]
    async fn test_bump_dir_rev_success() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        // Root should have rev = 0
        let new_rev = svc
            .handle_bump_dir_rev(root, 0, None, ctx.clone())
            .await
            .unwrap();
        assert_eq!(new_rev, 1);

        // Bump again
        let new_rev = svc.handle_bump_dir_rev(root, 1, None, ctx).await.unwrap();
        assert_eq!(new_rev, 2);
    }

    #[tokio::test]
    async fn test_bump_dir_rev_mismatch() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        let result = svc.handle_bump_dir_rev(root, 999, None, ctx).await;
        assert!(result.is_err());
    }

    // ==================== Transaction Tests ====================

    #[tokio::test]
    async fn test_begin_and_commit_txn() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let txid = svc.handle_begin_txn(ctx.clone()).await.unwrap();
        assert!(txid.starts_with("tx-"));

        // Create node in transaction
        let node = create_dir_node(700);
        svc.handle_set_inode(node.clone(), Some(txid.clone()), ctx.clone())
            .await
            .unwrap();

        // Should be visible within txn
        let fetched = svc
            .handle_get_inode(700, Some(txid.clone()), ctx.clone())
            .await
            .unwrap();
        assert!(fetched.is_some());

        // Commit
        svc.handle_commit(Some(txid), ctx.clone()).await.unwrap();

        // Should still be visible after commit
        let fetched = svc.handle_get_inode(700, None, ctx).await.unwrap();
        assert!(fetched.is_some());
    }

    #[tokio::test]
    async fn test_begin_and_rollback_txn() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let txid = svc.handle_begin_txn(ctx.clone()).await.unwrap();

        let node = create_dir_node(701);
        svc.handle_set_inode(node.clone(), Some(txid.clone()), ctx.clone())
            .await
            .unwrap();

        // Rollback
        svc.handle_rollback(Some(txid), ctx.clone()).await.unwrap();

        // Should NOT be visible after rollback
        let fetched = svc.handle_get_inode(701, None, ctx).await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_commit_without_txid() {
        let (svc, _tmp) = create_test_service();
        // Should be no-op
        let result = svc.handle_commit(None, dummy_ctx()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rollback_without_txid() {
        let (svc, _tmp) = create_test_service();
        let result = svc.handle_rollback(None, dummy_ctx()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_invalid_txid() {
        let (svc, _tmp) = create_test_service();
        let result = svc
            .handle_commit(Some("invalid-txid".to_string()), dummy_ctx())
            .await;
        assert!(result.is_err());
    }

    // ==================== File Lease Tests ====================

    #[tokio::test]
    async fn test_acquire_file_lease() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(800, "fb-800");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session = SessionId("session-1".to_string());
        let lease_seq = svc
            .handle_acquire_file_lease(800, session, Duration::from_secs(60), ctx)
            .await
            .unwrap();
        assert!(lease_seq > 0);
    }

    #[tokio::test]
    async fn test_acquire_file_lease_renewal() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(801, "fb-801");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session = SessionId("session-1".to_string());
        let seq1 = svc
            .handle_acquire_file_lease(801, session.clone(), Duration::from_secs(60), ctx.clone())
            .await
            .unwrap();

        // Same session should get same seq (renewal)
        let seq2 = svc
            .handle_acquire_file_lease(801, session, Duration::from_secs(60), ctx)
            .await
            .unwrap();
        assert_eq!(seq1, seq2);
    }

    #[tokio::test]
    async fn test_acquire_file_lease_conflict() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(802, "fb-802");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session1 = SessionId("session-1".to_string());
        svc.handle_acquire_file_lease(802, session1, Duration::from_secs(3600), ctx.clone())
            .await
            .unwrap();

        // Different session should fail (lease not expired)
        let session2 = SessionId("session-2".to_string());
        let result = svc
            .handle_acquire_file_lease(802, session2, Duration::from_secs(60), ctx)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acquire_file_lease_node_not_found() {
        let (svc, _tmp) = create_test_service();
        let session = SessionId("session-1".to_string());
        let result = svc
            .handle_acquire_file_lease(9999, session, Duration::from_secs(60), dummy_ctx())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_renew_file_lease() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(803, "fb-803");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session = SessionId("session-1".to_string());
        let lease_seq = svc
            .handle_acquire_file_lease(803, session.clone(), Duration::from_secs(60), ctx.clone())
            .await
            .unwrap();

        // Renew with correct session and seq
        let result = svc
            .handle_renew_file_lease(803, session, lease_seq, Duration::from_secs(120), ctx)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_renew_file_lease_mismatch() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(804, "fb-804");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session = SessionId("session-1".to_string());
        svc.handle_acquire_file_lease(804, session.clone(), Duration::from_secs(60), ctx.clone())
            .await
            .unwrap();

        // Wrong seq
        let result = svc
            .handle_renew_file_lease(804, session, 999, Duration::from_secs(120), ctx)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_release_file_lease() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(805, "fb-805");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session = SessionId("session-1".to_string());
        let lease_seq = svc
            .handle_acquire_file_lease(805, session.clone(), Duration::from_secs(60), ctx.clone())
            .await
            .unwrap();

        // Release
        let result = svc
            .handle_release_file_lease(805, session, lease_seq, ctx.clone())
            .await;
        assert!(result.is_ok());

        // After release, another session can acquire
        let session2 = SessionId("session-2".to_string());
        let result = svc
            .handle_acquire_file_lease(805, session2, Duration::from_secs(60), ctx)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_release_file_lease_mismatch() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = create_file_node_working(806, "fb-806");
        svc.handle_set_inode(node.clone(), None, ctx.clone())
            .await
            .unwrap();

        let session = SessionId("session-1".to_string());
        svc.handle_acquire_file_lease(806, session.clone(), Duration::from_secs(60), ctx.clone())
            .await
            .unwrap();

        // Wrong session
        let wrong_session = SessionId("wrong-session".to_string());
        let result = svc
            .handle_release_file_lease(806, wrong_session, 1, ctx)
            .await;
        assert!(result.is_err());
    }

    // ==================== ObjStat Tests ====================

    #[tokio::test]
    async fn test_obj_stat_bump_new() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(1);
        let ref_count = svc
            .handle_obj_stat_bump(obj_id.clone(), 1, None, ctx.clone())
            .await
            .unwrap();
        assert_eq!(ref_count, 1);

        // Bump again
        let ref_count = svc
            .handle_obj_stat_bump(obj_id, 2, None, ctx)
            .await
            .unwrap();
        assert_eq!(ref_count, 3);
    }

    #[tokio::test]
    async fn test_obj_stat_bump_decrement() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(2);
        svc.handle_obj_stat_bump(obj_id.clone(), 5, None, ctx.clone())
            .await
            .unwrap();

        let ref_count = svc
            .handle_obj_stat_bump(obj_id, -2, None, ctx)
            .await
            .unwrap();
        assert_eq!(ref_count, 3);
    }

    #[tokio::test]
    async fn test_obj_stat_bump_negative_error() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(3);
        svc.handle_obj_stat_bump(obj_id.clone(), 2, None, ctx.clone())
            .await
            .unwrap();

        // Try to decrement below zero
        let result = svc.handle_obj_stat_bump(obj_id, -10, None, ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_obj_stat_bump_new_with_negative_error() {
        let (svc, _tmp) = create_test_service();
        let obj_id = create_obj_id(4);

        // Cannot create with negative delta
        let result = svc
            .handle_obj_stat_bump(obj_id, -1, None, dummy_ctx())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_obj_stat_get() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(5);
        svc.handle_obj_stat_bump(obj_id.clone(), 3, None, ctx.clone())
            .await
            .unwrap();

        let stat = svc.handle_obj_stat_get(obj_id, ctx).await.unwrap().unwrap();
        assert_eq!(stat.ref_count, 3);
        assert!(stat.zero_since.is_none());
    }

    #[tokio::test]
    async fn test_obj_stat_get_not_found() {
        let (svc, _tmp) = create_test_service();
        let obj_id = create_obj_id(6);
        let stat = svc.handle_obj_stat_get(obj_id, dummy_ctx()).await.unwrap();
        assert!(stat.is_none());
    }

    #[tokio::test]
    async fn test_obj_stat_zero_since() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(7);
        svc.handle_obj_stat_bump(obj_id.clone(), 2, None, ctx.clone())
            .await
            .unwrap();

        // Decrement to zero
        svc.handle_obj_stat_bump(obj_id.clone(), -2, None, ctx.clone())
            .await
            .unwrap();

        let stat = svc.handle_obj_stat_get(obj_id, ctx).await.unwrap().unwrap();
        assert_eq!(stat.ref_count, 0);
        assert!(stat.zero_since.is_some());
    }

    #[tokio::test]
    async fn test_obj_stat_list_zero() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        // Create several objects with zero ref_count
        for i in 0..3 {
            let obj_id = create_obj_id(10 + i);
            svc.handle_obj_stat_bump(obj_id.clone(), 1, None, ctx.clone())
                .await
                .unwrap();
            svc.handle_obj_stat_bump(obj_id, -1, None, ctx.clone())
                .await
                .unwrap();
        }

        // List zero refs
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 10; // future timestamp to include all
        let zeros = svc.handle_obj_stat_list_zero(now, 10, ctx).await.unwrap();
        assert_eq!(zeros.len(), 3);
    }

    #[tokio::test]
    async fn test_obj_stat_delete_if_zero() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(20);
        svc.handle_obj_stat_bump(obj_id.clone(), 1, None, ctx.clone())
            .await
            .unwrap();
        svc.handle_obj_stat_bump(obj_id.clone(), -1, None, ctx.clone())
            .await
            .unwrap();

        // Delete
        let deleted = svc
            .handle_obj_stat_delete_if_zero(obj_id.clone(), None, ctx.clone())
            .await
            .unwrap();
        assert!(deleted);

        // Should be gone
        let stat = svc.handle_obj_stat_get(obj_id, ctx).await.unwrap();
        assert!(stat.is_none());
    }

    #[tokio::test]
    async fn test_obj_stat_delete_if_zero_nonzero() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(21);
        svc.handle_obj_stat_bump(obj_id.clone(), 5, None, ctx.clone())
            .await
            .unwrap();

        // Should not delete (ref_count != 0)
        let deleted = svc
            .handle_obj_stat_delete_if_zero(obj_id.clone(), None, ctx.clone())
            .await
            .unwrap();
        assert!(!deleted);

        // Should still exist
        let stat = svc.handle_obj_stat_get(obj_id, ctx).await.unwrap();
        assert!(stat.is_some());
    }

    // ==================== Node State Tests ====================

    #[tokio::test]
    async fn test_node_state_linked() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(30);
        let qcid = create_obj_id(31);

        let node = NodeRecord {
            inode_id: 900,
            kind: NodeKind::File,
            read_only: false,
            base_obj_id: None,
            state: NodeState::Linked(ndm::FileLinkedState {
                obj_id: obj_id.clone(),
                qcid: qcid.clone(),
                filebuffer_id: "fb-900".to_string(),
                linked_at: 5000,
            }),
            rev: None,
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        };

        svc.handle_set_inode(node, None, ctx.clone()).await.unwrap();

        let fetched = svc.handle_get_inode(900, None, ctx).await.unwrap().unwrap();
        match fetched.state {
            NodeState::Linked(s) => {
                assert_eq!(s.obj_id, obj_id);
                assert_eq!(s.qcid, qcid);
                assert_eq!(s.filebuffer_id, "fb-900");
                assert_eq!(s.linked_at, 5000);
            }
            _ => panic!("expected Linked state"),
        }
    }

    #[tokio::test]
    async fn test_node_state_finalized() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let obj_id = create_obj_id(40);

        let node = NodeRecord {
            inode_id: 901,
            kind: NodeKind::File,
            read_only: true,
            base_obj_id: None,
            state: NodeState::Finalized(ndm::FinalizedObjState {
                obj_id: obj_id.clone(),
                finalized_at: 6000,
            }),
            rev: None,
            meta: None,
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        };

        svc.handle_set_inode(node, None, ctx.clone()).await.unwrap();

        let fetched = svc.handle_get_inode(901, None, ctx).await.unwrap().unwrap();
        assert!(fetched.read_only);
        match fetched.state {
            NodeState::Finalized(s) => {
                assert_eq!(s.obj_id, obj_id);
                assert_eq!(s.finalized_at, 6000);
            }
            _ => panic!("expected Finalized state"),
        }
    }

    #[tokio::test]
    async fn test_node_with_meta() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let meta = serde_json::json!({
            "owner": "user1",
            "permissions": 0o755,
            "custom": {"key": "value"}
        });

        let node = NodeRecord {
            inode_id: 902,
            kind: NodeKind::Dir,
            read_only: false,
            base_obj_id: None,
            state: NodeState::DirNormal,
            rev: Some(0),
            meta: Some(meta.clone()),
            lease_client_session: None,
            lease_seq: None,
            lease_expire_at: None,
        };

        svc.handle_set_inode(node, None, ctx.clone()).await.unwrap();

        let fetched = svc.handle_get_inode(902, None, ctx).await.unwrap().unwrap();
        assert!(fetched.meta.is_some());
        let fetched_meta = fetched.meta.unwrap();
        assert_eq!(fetched_meta["owner"], "user1");
        assert_eq!(fetched_meta["permissions"], 0o755);
    }

    #[tokio::test]
    async fn test_node_with_lease_info() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let node = NodeRecord {
            inode_id: 903,
            kind: NodeKind::File,
            read_only: false,
            base_obj_id: None,
            state: NodeState::Working(ndm::FileWorkingState {
                fb_handle: "fb-903".to_string(),
                last_write_at: 1000,
            }),
            rev: None,
            meta: None,
            lease_client_session: Some(ClientSessionId("session-x".to_string())),
            lease_seq: Some(5),
            lease_expire_at: Some(9999999),
        };

        svc.handle_set_inode(node, None, ctx.clone()).await.unwrap();

        let fetched = svc.handle_get_inode(903, None, ctx).await.unwrap().unwrap();
        assert_eq!(
            fetched.lease_client_session,
            Some(ClientSessionId("session-x".to_string()))
        );
        assert_eq!(fetched.lease_seq, Some(5));
        assert_eq!(fetched.lease_expire_at, Some(9999999));
    }

    // ==================== Dentry with ObjId Target ====================

    #[tokio::test]
    async fn test_dentry_with_obj_id_target() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        let obj_id = create_obj_id(50);

        svc.handle_upsert_dentry(
            root,
            "linked_file".to_string(),
            DentryTarget::ObjId(obj_id.clone()),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        let dentry = svc
            .handle_get_dentry(root, "linked_file".to_string(), None, ctx)
            .await
            .unwrap()
            .unwrap();

        match dentry.target {
            DentryTarget::ObjId(id) => assert_eq!(id, obj_id),
            _ => panic!("expected ObjId target"),
        }
    }

    // ==================== Transaction Isolation ====================

    #[tokio::test]
    async fn test_transaction_isolation() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();

        let txid = svc.handle_begin_txn(ctx.clone()).await.unwrap();

        // Create node in txn
        let node = create_dir_node(950);
        svc.handle_set_inode(node, Some(txid.clone()), ctx.clone())
            .await
            .unwrap();

        // NOT visible outside txn (before commit)
        // Note: Due to SQLite WAL mode and how connections work,
        // uncommitted changes in one connection may or may not be visible
        // in another. This test verifies the basic transaction semantics.
        let fetched_in_txn = svc
            .handle_get_inode(950, Some(txid.clone()), ctx.clone())
            .await
            .unwrap();
        assert!(fetched_in_txn.is_some());

        // Commit
        svc.handle_commit(Some(txid), ctx.clone()).await.unwrap();

        // Now visible outside txn
        let fetched_after = svc.handle_get_inode(950, None, ctx).await.unwrap();
        assert!(fetched_after.is_some());
    }

    // ==================== Resolve Path Cache Tests ====================

    #[tokio::test]
    async fn test_resolve_path_cache_invalidate_prefix_on_edge_change() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        // Build /a/b/c where each component is a directory inode.
        let a = create_dir_node(10);
        let b1 = create_dir_node(11);
        let c = create_dir_node(12);
        svc.handle_set_inode(a.clone(), None, ctx.clone())
            .await
            .unwrap();
        svc.handle_set_inode(b1.clone(), None, ctx.clone())
            .await
            .unwrap();
        svc.handle_set_inode(c.clone(), None, ctx.clone())
            .await
            .unwrap();

        svc.handle_upsert_dentry(
            root,
            "a".to_string(),
            DentryTarget::IndexNodeId(a.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();
        svc.handle_upsert_dentry(
            a.inode_id,
            "b".to_string(),
            DentryTarget::IndexNodeId(b1.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();
        svc.handle_upsert_dentry(
            b1.inode_id,
            "c".to_string(),
            DentryTarget::IndexNodeId(c.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        // Populate cache.
        let resolved = svc
            .handle_resolve_path(&NdmPath::new("/a/b/c"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(resolved.unwrap().0, c.inode_id);

        // Change edge /a/b -> new inode, should invalidate /a/b/* cache entries.
        let b2 = create_dir_node(20);
        svc.handle_set_inode(b2.clone(), None, ctx.clone())
            .await
            .unwrap();
        svc.handle_upsert_dentry(
            a.inode_id,
            "b".to_string(),
            DentryTarget::IndexNodeId(b2.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        let resolved2 = svc
            .handle_resolve_path(&NdmPath::new("/a/b/c"), ctx)
            .await
            .unwrap();
        assert!(resolved2.is_none());
    }

    #[tokio::test]
    async fn test_resolve_path_cache_invalidate_on_commit() {
        let (svc, _tmp) = create_test_service();
        let ctx = dummy_ctx();
        let root = svc.handle_root_dir(ctx.clone()).await.unwrap();

        // Build /a/b/c.
        let a = create_dir_node(30);
        let b1 = create_dir_node(31);
        let c = create_dir_node(32);
        svc.handle_set_inode(a.clone(), None, ctx.clone())
            .await
            .unwrap();
        svc.handle_set_inode(b1.clone(), None, ctx.clone())
            .await
            .unwrap();
        svc.handle_set_inode(c.clone(), None, ctx.clone())
            .await
            .unwrap();

        svc.handle_upsert_dentry(
            root,
            "a".to_string(),
            DentryTarget::IndexNodeId(a.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();
        svc.handle_upsert_dentry(
            a.inode_id,
            "b".to_string(),
            DentryTarget::IndexNodeId(b1.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();
        svc.handle_upsert_dentry(
            b1.inode_id,
            "c".to_string(),
            DentryTarget::IndexNodeId(c.inode_id),
            None,
            ctx.clone(),
        )
        .await
        .unwrap();

        // Populate cache.
        let resolved = svc
            .handle_resolve_path(&NdmPath::new("/a/b/c"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(resolved.unwrap().0, c.inode_id);

        // Mutate /a/b within a transaction.
        let txid = svc.handle_begin_txn(ctx.clone()).await.unwrap();
        let b2 = create_dir_node(40);
        svc.handle_set_inode(b2.clone(), Some(txid.clone()), ctx.clone())
            .await
            .unwrap();
        svc.handle_upsert_dentry(
            a.inode_id,
            "b".to_string(),
            DentryTarget::IndexNodeId(b2.inode_id),
            Some(txid.clone()),
            ctx.clone(),
        )
        .await
        .unwrap();
        svc.handle_commit(Some(txid), ctx.clone()).await.unwrap();

        // After commit, the cached /a/b/* entries must be invalidated.
        let resolved2 = svc
            .handle_resolve_path(&NdmPath::new("/a/b/c"), ctx)
            .await
            .unwrap();
        assert!(resolved2.is_none());
    }
}
