use super::super::test_support::test_persistent_coordinator;
use super::super::ConversationCoordinator;
use crate::agentic::core::{Message, SessionConfig};
use crate::agentic::execution::{ExecutionResult, FinishReason};
use crate::agentic::session::SessionManager;
use crate::service::session::TurnStatus;
use crate::util::errors::OpenBitFunError;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

async fn complete_observed_turn(
    coordinator: &ConversationCoordinator,
    manager: &SessionManager,
    session_id: &str,
    turn_id: &str,
    message: Message,
) {
    ConversationCoordinator::persist_completed_dialog_turn(
        coordinator.event_queue.as_ref(),
        manager,
        None,
        session_id,
        turn_id,
        &ExecutionResult {
            final_message: message.clone(),
            total_rounds: 1,
            success: true,
            new_messages: vec![message],
            finish_reason: FinishReason::Complete,
            total_tools: 0,
            duration_ms: 1,
            partial_recovery_reason: None,
            effective_finish_reason: "complete".to_string(),
            has_final_response: true,
        },
        None,
    )
    .await;
}

#[tokio::test]
async fn load_relay_session_turns_reads_history_after_the_session_is_unloaded() {
    let workspace = tempfile::tempdir().expect("workspace");
    crate::service::workspace::legacy_compat::register_local_fixture_blocking(workspace.path());
    let (coordinator, session_manager) = test_persistent_coordinator();
    let session = session_manager
        .create_session(
            "Host stream observer".to_string(),
            "Standard".to_string(),
            SessionConfig {
                tool_presentation_mode: Default::default(),
                workspace_path: Some(workspace.path().to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("create session");
    let storage = session_manager
        .effective_session_storage_path(&session.session_id)
        .await
        .expect("session storage path");
    let turn_id = session_manager
        .start_dialog_turn(
            &session.session_id,
            "Standard".to_string(),
            "observe".to_string(),
            Some("turn-host-stream-observer".to_string()),
            None,
            None,
        )
        .await
        .expect("start turn");
    let message = Message::assistant("observer history".to_string())
        .with_turn_id(turn_id.clone())
        .with_round_id("round-observer".to_string());
    complete_observed_turn(
        &coordinator,
        &session_manager,
        &session.session_id,
        &turn_id,
        message,
    )
    .await;

    session_manager
        .unload_session_from_memory(&session.session_id)
        .await
        .expect("unload writer");
    assert!(session_manager.get_session(&session.session_id).is_none());

    let turns = coordinator
        .load_relay_session_turns(&storage, &session.session_id, None)
        .await
        .expect("host streams must read persisted history without a loaded writer");
    assert_eq!(
        turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-host-stream-observer"]
    );
    let one = coordinator
        .load_relay_session_turns(&storage, &session.session_id, Some(&turn_id))
        .await
        .expect("single-turn host-stream sync must not require an in-memory session");
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].turn_id, turn_id);

    let (page, next) = coordinator
        .load_relay_history_turn(&storage, &session.session_id, None)
        .await
        .expect("paged history must not require an in-memory writer");
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        serde_json::to_value(&one).unwrap()
    );
    assert_eq!(next, None);

    session_manager
        .restore_session(workspace.path(), &session.session_id)
        .await
        .expect("restore for cleanup");
    session_manager
        .delete_session_by_id(&session.session_id)
        .await
        .expect("clean up persisted test session");
}

async fn relay_observer_fixture(
    label: &str,
) -> (
    tempfile::TempDir,
    ConversationCoordinator,
    Arc<SessionManager>,
    String,
    PathBuf,
    String,
) {
    let workspace = tempfile::tempdir().expect("workspace");
    crate::service::workspace::legacy_compat::register_local_fixture_blocking(workspace.path());
    let (coordinator, session_manager) = test_persistent_coordinator();
    let session = session_manager
        .create_session(
            label.to_string(),
            "Standard".to_string(),
            SessionConfig {
                tool_presentation_mode: Default::default(),
                workspace_path: Some(workspace.path().to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("create session");
    let storage = session_manager
        .effective_session_storage_path(&session.session_id)
        .await
        .expect("session storage path");
    let turn_id = session_manager
        .start_dialog_turn(
            &session.session_id,
            "Standard".to_string(),
            "observe".to_string(),
            Some(format!("turn-{label}")),
            None,
            None,
        )
        .await
        .expect("start turn");
    let message = Message::assistant("observer history".to_string())
        .with_turn_id(turn_id.clone())
        .with_round_id("round-observer".to_string());
    complete_observed_turn(
        &coordinator,
        &session_manager,
        &session.session_id,
        &turn_id,
        message,
    )
    .await;
    (
        workspace,
        coordinator,
        session_manager,
        session.session_id,
        storage,
        turn_id,
    )
}

#[tokio::test]
async fn load_relay_session_turns_streams_and_replays_a_temporary_btw_child() {
    let (workspace, coordinator, manager, parent, storage, parent_turn) =
        relay_observer_fixture("btw-host").await;
    let child_id = format!("btw-{parent}");
    coordinator
        .ensure_btw_session(
            &parent,
            &child_id,
            None,
            "side-question",
            Some(&parent_turn),
            Some(0),
        )
        .await
        .unwrap();

    let history = |before| coordinator.load_relay_history_turn(&storage, &child_id, before);
    assert!(history(None).await.unwrap().0.is_empty());
    for (id, question, reply) in [
        ("side-1", "first question", "first answer"),
        ("side-2", "follow-up", "second answer"),
    ] {
        manager
            .start_dialog_turn(
                &child_id,
                "Standard".into(),
                question.into(),
                Some(id.into()),
                None,
                None,
            )
            .await
            .unwrap();
        let message = Message::assistant(reply.to_string())
            .with_turn_id(id.to_string())
            .with_round_id(format!("round-{id}"));
        manager
            .add_message(&child_id, message.clone())
            .await
            .unwrap();
        {
            let _executing = manager.acquire_session_mutation(&child_id).await.unwrap();
            let live = tokio::time::timeout(
                Duration::from_secs(1),
                coordinator.load_relay_session_turns(&storage, &child_id, Some(id)),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(live.len(), 1);
            assert_eq!(live[0].user_message.content, question);
            assert_eq!(live[0].status, TurnStatus::InProgress);
            assert_eq!(live[0].model_rounds[0].text_items[0].content, reply);
        }
        complete_observed_turn(&coordinator, &manager, &child_id, id, message).await;
    }

    let (latest, before) = history(None).await.unwrap();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].turn_id, "side-2");
    assert_eq!(latest[0].status, TurnStatus::Completed);
    assert_eq!(
        latest[0].model_rounds[0].text_items[0].content,
        "second answer"
    );
    assert_eq!(before, Some(1));
    let (first, before) = history(before).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].turn_id, "side-1");
    assert_eq!(before, None);
    assert!(history(Some(0)).await.unwrap().0.is_empty());
    let all = coordinator
        .load_relay_session_turns(&storage, &child_id, None)
        .await
        .unwrap();
    assert_eq!(
        all.iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        ["side-1", "side-2"]
    );
    assert!(matches!(
        coordinator
            .load_relay_session_turns(&storage, &child_id, Some(&parent_turn))
            .await,
        Err(OpenBitFunError::NotFound(_))
    ));
    assert!(matches!(
        coordinator
            .load_relay_history_turn(&workspace.path().join("other"), &child_id, None)
            .await,
        Err(OpenBitFunError::Validation(_))
    ));
    assert!(manager
        .persistence_manager()
        .load_visible_session_turns(&storage, &child_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn load_relay_session_turns_projects_history_while_a_turn_holds_the_mutation_lease() {
    let (_workspace, coordinator, session_manager, session_id, storage, turn_id) =
        relay_observer_fixture("lease-held-observer").await;
    // A Turn waiting on an unanswered question keeps this lease for its
    // whole life; the observer read must not wait for it to finish.
    let _turn_lease = session_manager
        .acquire_session_mutation(&session_id)
        .await
        .expect("turn lease");
    let turns = tokio::time::timeout(
        super::super::RELAY_HISTORY_OBSERVER_LEASE_WAIT + Duration::from_secs(10),
        coordinator.load_relay_session_turns(&storage, &session_id, None),
    )
    .await
    .expect("observer read must not wait for the lease owner")
    .expect("observer read projects persisted history");
    assert_eq!(
        turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec![turn_id.as_str()]
    );

    // A staged undo is a stable visible projection and stays readable.
    session_manager
        .persistence_manager()
        .save_session_revert_state(
            &storage,
            &session_id,
            &crate::agentic::session::revert::SessionRevertState {
                schema_version: crate::agentic::session::revert::SESSION_REVERT_SCHEMA_VERSION,
                boundary_turn: 0,
                original_turn_end: 1,
                phase: crate::agentic::session::revert::SessionRevertPhase::Staged,
                workspace_checkpoint: Vec::new(),
            },
        )
        .await
        .expect("stage revert");
    let staged = coordinator
        .load_relay_session_turns(&storage, &session_id, None)
        .await
        .expect("staged undo remains readable without the lease");
    assert!(staged.is_empty(), "staged undo hides the reverted suffix");
}

#[tokio::test]
async fn load_relay_session_turns_keep_owner_restart_reclamation_without_the_mutation_lease() {
    let (_workspace, coordinator, session_manager, session_id, storage, _turn_id) =
        relay_observer_fixture("restart-reclamation").await;
    session_manager
        .unload_session_from_memory(&session_id)
        .await
        .expect("unload the previous owner");
    let mut live = crate::service::session::DialogTurnData::new(
        "turn-left-running".to_string(),
        1,
        session_id.clone(),
        crate::service::session::UserMessageData {
            id: "user-left-running".to_string(),
            content: "left running".to_string(),
            timestamp: 0,
            metadata: None,
        },
    );
    live.status = TurnStatus::InProgress;
    session_manager
        .persistence_manager()
        .save_dialog_turn(&storage, &live)
        .await
        .expect("persist a turn left running by a stopped owner");

    async fn read(
        coordinator: &ConversationCoordinator,
        storage: &Path,
        session_id: &str,
    ) -> crate::service::session::DialogTurnData {
        coordinator
            .load_relay_session_turns(storage, session_id, None)
            .await
            .expect("observer read")
            .into_iter()
            .find(|turn| turn.turn_id == "turn-left-running")
            .expect("running turn is visible")
    }

    // No owner holds the writer: both read paths project the interruption.
    let leased = read(&coordinator, &storage, &session_id).await;
    let mutation = session_manager
        .acquire_session_mutation(&session_id)
        .await
        .expect("in-process mutation lease");
    let lease_free = read(&coordinator, &storage, &session_id).await;
    for turn in [&leased, &lease_free] {
        assert_eq!(turn.status, TurnStatus::Cancelled);
        assert_eq!(turn.finish_reason.as_deref(), Some("interrupted"));
    }

    // Another owner holds the writer: absence from memory is not proof of
    // interruption on either path.
    let writer = session_manager
        .persistence_manager()
        .lock_session_writes(&storage, &session_id)
        .expect("simulated live owner");
    let lease_free = read(&coordinator, &storage, &session_id).await;
    drop(mutation);
    let leased = read(&coordinator, &storage, &session_id).await;
    for turn in [&leased, &lease_free] {
        assert_eq!(turn.status, TurnStatus::InProgress);
        assert_eq!(turn.finish_reason, None);
    }
    drop(writer);

    // Observer projection never rewrites the persisted record.
    let persisted = session_manager
        .persistence_manager()
        .load_visible_session_turns(&storage, &session_id)
        .await
        .expect("persisted turns");
    assert!(persisted
        .iter()
        .any(|turn| turn.turn_id == "turn-left-running" && turn.status == TurnStatus::InProgress));
}

#[tokio::test]
async fn load_relay_session_turns_refuses_unfinished_undo_without_the_mutation_lease() {
    let (_workspace, coordinator, session_manager, session_id, storage, _turn_id) =
        relay_observer_fixture("lease-held-undo").await;
    for phase in [
        crate::agentic::session::revert::SessionRevertPhase::Applying,
        crate::agentic::session::revert::SessionRevertPhase::Clearing,
        crate::agentic::session::revert::SessionRevertPhase::Committing,
    ] {
        session_manager
            .persistence_manager()
            .save_session_revert_state(
                &storage,
                &session_id,
                &crate::agentic::session::revert::SessionRevertState {
                    schema_version: crate::agentic::session::revert::SESSION_REVERT_SCHEMA_VERSION,
                    boundary_turn: 0,
                    original_turn_end: 1,
                    phase,
                    workspace_checkpoint: Vec::new(),
                },
            )
            .await
            .expect("persist unfinished undo");
        let _writer = session_manager
            .acquire_session_mutation(&session_id)
            .await
            .expect("undo writer lease");
        let error = coordinator
            .load_relay_session_turns(&storage, &session_id, None)
            .await
            .expect_err("an unfinished undo must not be projected without the lease");
        assert!(
            matches!(error, OpenBitFunError::OutcomeUnknown(_)),
            "unexpected error for {phase:?}: {error:?}"
        );
    }
}

#[tokio::test]
async fn load_relay_session_turns_preserves_external_executor_activity() {
    use crate::agentic::core::SessionState;
    use crate::agentic::events::AgenticEvent;
    use crate::service::session::{DialogTurnData, TurnStatus, UserMessageData};

    let workspace = tempfile::tempdir().unwrap();
    crate::service::workspace::legacy_compat::register_local_fixture_blocking(workspace.path());
    let (coordinator, manager) = test_persistent_coordinator();
    let session = manager
        .create_session(
            "External executor".into(),
            "Standard".into(),
            SessionConfig {
                workspace_path: Some(workspace.path().to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let storage = manager
        .effective_session_storage_path(&session.session_id)
        .await
        .unwrap();
    // Native drivers publish dialog lifecycle through EventQueue, not Core's
    // execution engine. Their UI checkpoints remain InProgress between rounds.
    let mut turn = DialogTurnData::new(
        "external-turn".into(),
        0,
        session.session_id.clone(),
        UserMessageData {
            id: "user".into(),
            content: "work".into(),
            timestamp: 1,
            metadata: None,
        },
    );
    manager
        .persistence_manager()
        .save_dialog_turn(&storage, &turn)
        .await
        .unwrap();
    coordinator
        .event_queue
        .enqueue(
            AgenticEvent::DialogTurnStarted {
                session_id: session.session_id.clone(),
                turn_id: turn.turn_id.clone(),
                turn_index: 0,
                user_input: "work".into(),
                original_user_input: None,
                user_message_metadata: None,
                session_binding: None,
            },
            None,
        )
        .await
        .unwrap();
    coordinator.event_queue.dequeue_batch(100).await;
    assert!(matches!(
        manager.get_session(&session.session_id).unwrap().state,
        SessionState::Idle
    ));

    // Both a loaded-but-idle Core shell and an unloaded Core session must
    // preserve the external executor, including a fresh peer attachment.
    for unloaded in [false, true] {
        if unloaded {
            manager
                .unload_session_from_memory(&session.session_id)
                .await
                .unwrap();
        }
        let all = coordinator
            .load_relay_session_turns(&storage, &session.session_id, None)
            .await
            .unwrap();
        let live = coordinator
            .load_relay_session_turns(&storage, &session.session_id, Some(&turn.turn_id))
            .await
            .unwrap();
        let (page, _) = coordinator
            .load_relay_history_turn(&storage, &session.session_id, None)
            .await
            .unwrap();
        for projection in [all, live, page] {
            assert_eq!(projection[0].status, TurnStatus::InProgress);
            assert!(projection[0].finish_reason.is_none());
            assert!(projection[0].error.is_none());
            assert!(projection[0].end_time.is_none());
        }
    }

    // Execution evidence is process-local: a new host still identifies
    // abandoned checkpoints, without rewriting the persisted user record.
    let (restarted, _) = test_persistent_coordinator();
    let abandoned = restarted
        .load_relay_session_turns(&storage, &session.session_id, None)
        .await
        .unwrap();
    assert_eq!(abandoned[0].status, TurnStatus::Cancelled);
    assert_eq!(abandoned[0].finish_reason.as_deref(), Some("interrupted"));
    let stored = manager
        .persistence_manager()
        .load_visible_session_turn(&storage, &session.session_id, &turn.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, TurnStatus::InProgress);

    // An actual terminal record stays terminal, even if publication of its
    // lifecycle event has not caught up with persistence yet.
    turn.mark_completed();
    manager
        .persistence_manager()
        .save_dialog_turn(&storage, &turn)
        .await
        .unwrap();
    let completed = coordinator
        .load_relay_session_turns(&storage, &session.session_id, None)
        .await
        .unwrap();
    assert_eq!(completed[0].status, TurnStatus::Completed);
    manager
        .restore_session(workspace.path(), &session.session_id)
        .await
        .unwrap();
    manager
        .delete_session_by_id(&session.session_id)
        .await
        .unwrap();
}
