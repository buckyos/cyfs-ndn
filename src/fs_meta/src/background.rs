// ========== Background Manager ==========

use async_trait::async_trait;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackgroundTaskKind {
    FinalizeDir,
    LazyMigration,
}

impl BackgroundTaskKind {
    fn all() -> [BackgroundTaskKind; 2] {
        [
            BackgroundTaskKind::FinalizeDir,
            BackgroundTaskKind::LazyMigration,
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BackgroundTask {
    pub kind: BackgroundTaskKind,
    pub path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueuePushMode {
    Front,
    Back,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackgroundTaskPolicy {
    /// How often this kind of task should be attempted.
    pub interval: Duration,
    /// Max cumulative execution time in one sweep.
    pub max_run_time: Duration,
}

impl BackgroundTaskPolicy {
    pub fn new(interval: Duration, max_run_time: Duration) -> Self {
        Self {
            interval,
            max_run_time,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct BackgroundRunStats {
    pub executed: usize,
    pub failed: usize,
    pub skipped_due_to_budget: usize,
}

#[async_trait]
pub trait BackgroundTaskExecutor: Send + Sync {
    async fn execute(&self, task: BackgroundTask) -> Result<(), String>;
}

#[derive(Default)]
struct TaskQueue {
    items: VecDeque<BackgroundTask>,
    dedup: HashSet<BackgroundTask>,
}

impl TaskQueue {
    fn push(&mut self, task: BackgroundTask, mode: QueuePushMode) {
        if self.dedup.contains(&task) {
            if let Some(pos) = self.items.iter().position(|t| t == &task) {
                let _ = self.items.remove(pos);
            }
        } else {
            let _ = self.dedup.insert(task.clone());
        }

        match mode {
            QueuePushMode::Front => self.items.push_front(task),
            QueuePushMode::Back => self.items.push_back(task),
        }
    }

    fn pop_front(&mut self) -> Option<BackgroundTask> {
        let task = self.items.pop_front()?;
        let _ = self.dedup.remove(&task);
        Some(task)
    }

    fn snapshot(&self) -> Vec<BackgroundTask> {
        self.items.iter().cloned().collect()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

pub struct BackgroundMgr {
    queues: HashMap<BackgroundTaskKind, TaskQueue>,
    policies: HashMap<BackgroundTaskKind, BackgroundTaskPolicy>,
    last_attempt_at: HashMap<BackgroundTaskKind, Instant>,
}

impl BackgroundMgr {
    pub fn new() -> Self {
        let now = Instant::now();

        let mut queues = HashMap::new();
        let mut policies = HashMap::new();
        let mut last_attempt_at = HashMap::new();

        for kind in BackgroundTaskKind::all() {
            queues.insert(kind, TaskQueue::default());
            // Default strategy: try every 30s, and stop taking new tasks after 5s work.
            policies.insert(
                kind,
                BackgroundTaskPolicy::new(Duration::from_secs(30), Duration::from_secs(5)),
            );
            last_attempt_at.insert(kind, now);
        }

        Self {
            queues,
            policies,
            last_attempt_at,
        }
    }

    fn queue_mut(&mut self, kind: BackgroundTaskKind) -> &mut TaskQueue {
        self.queues.entry(kind).or_default()
    }

    fn queue_ref(&self, kind: BackgroundTaskKind) -> Option<&TaskQueue> {
        self.queues.get(&kind)
    }

    pub fn set_policy(&mut self, kind: BackgroundTaskKind, policy: BackgroundTaskPolicy) {
        self.policies.insert(kind, policy);
    }

    pub fn policy(&self, kind: BackgroundTaskKind) -> Option<BackgroundTaskPolicy> {
        self.policies.get(&kind).copied()
    }

    pub fn push_task(&mut self, task: BackgroundTask, mode: QueuePushMode) {
        self.queue_mut(task.kind).push(task, mode);
    }

    pub fn push_back(&mut self, kind: BackgroundTaskKind, path: impl Into<String>) {
        self.push_task(
            BackgroundTask {
                kind,
                path: path.into(),
            },
            QueuePushMode::Back,
        );
    }

    pub fn push_front(&mut self, kind: BackgroundTaskKind, path: impl Into<String>) {
        self.push_task(
            BackgroundTask {
                kind,
                path: path.into(),
            },
            QueuePushMode::Front,
        );
    }

    pub fn add_staged_task(&mut self, task: BackgroundTask) {
        self.push_task(task, QueuePushMode::Back);
    }

    pub fn add_lazy_migration_task(&mut self, task: BackgroundTask) {
        self.push_task(task, QueuePushMode::Back);
    }

    pub fn add_finalize_dir_task(&mut self, path: impl Into<String>) {
        self.push_back(BackgroundTaskKind::FinalizeDir, path);
    }

    pub fn staged_tasks(&self) -> Vec<BackgroundTask> {
        self.queue_ref(BackgroundTaskKind::FinalizeDir)
            .map(|q| q.snapshot())
            .unwrap_or_default()
    }

    pub fn lazy_migration_tasks(&self) -> Vec<BackgroundTask> {
        self.queue_ref(BackgroundTaskKind::LazyMigration)
            .map(|q| q.snapshot())
            .unwrap_or_default()
    }

    pub fn has_pending_tasks(&self) -> bool {
        BackgroundTaskKind::all()
            .into_iter()
            .any(|kind| self.queue_ref(kind).map(|q| !q.is_empty()).unwrap_or(false))
    }

    fn take_due_kinds(&mut self, now: Instant) -> Vec<(BackgroundTaskKind, BackgroundTaskPolicy)> {
        let mut due = Vec::new();
        for kind in BackgroundTaskKind::all() {
            let Some(policy) = self.policies.get(&kind).copied() else {
                continue;
            };
            let Some(last) = self.last_attempt_at.get(&kind).copied() else {
                continue;
            };
            if now.duration_since(last) < policy.interval {
                continue;
            }
            self.last_attempt_at.insert(kind, now);
            due.push((kind, policy));
        }
        due
    }

    fn pop_task_for_kind(&mut self, kind: BackgroundTaskKind) -> Option<BackgroundTask> {
        self.queue_mut(kind).pop_front()
    }

    pub async fn run_once(
        mgr: &Arc<Mutex<BackgroundMgr>>,
        executor: &(dyn BackgroundTaskExecutor + Send + Sync),
    ) -> BackgroundRunStats {
        let due = {
            match mgr.lock() {
                Ok(mut guard) => guard.take_due_kinds(Instant::now()),
                Err(e) => {
                    error!(
                        "background run_once: lock poisoned while taking due kinds: {}",
                        e
                    );
                    return BackgroundRunStats::default();
                }
            }
        };

        let mut stats = BackgroundRunStats::default();
        for (kind, policy) in due {
            let sweep_started = Instant::now();
            loop {
                if sweep_started.elapsed() >= policy.max_run_time {
                    break;
                }

                let task = {
                    match mgr.lock() {
                        Ok(mut guard) => guard.pop_task_for_kind(kind),
                        Err(e) => {
                            error!(
                                "background run_once: lock poisoned while popping task: {}",
                                e
                            );
                            None
                        }
                    }
                };

                let Some(task) = task else {
                    break;
                };

                let started = Instant::now();
                info!(
                    "background task start: kind={:?}, path={}",
                    task.kind, task.path
                );

                match executor.execute(task.clone()).await {
                    Ok(()) => {
                        stats.executed += 1;
                        info!(
                            "background task done: kind={:?}, path={}, elapsed={:?}",
                            task.kind,
                            task.path,
                            started.elapsed()
                        );
                    }
                    Err(e) => {
                        stats.failed += 1;
                        warn!(
                            "background task failed: kind={:?}, path={}, elapsed={:?}, err={}",
                            task.kind,
                            task.path,
                            started.elapsed(),
                            e
                        );
                    }
                }

                if sweep_started.elapsed() >= policy.max_run_time {
                    let skipped = {
                        match mgr.lock() {
                            Ok(guard) => guard
                                .queue_ref(kind)
                                .map(|q| q.items.len())
                                .unwrap_or(0usize),
                            Err(_) => 0usize,
                        }
                    };
                    stats.skipped_due_to_budget += skipped;
                    break;
                }
            }
        }

        stats
    }

    pub fn spawn_worker(
        mgr: Arc<Mutex<BackgroundMgr>>,
        executor: Arc<dyn BackgroundTaskExecutor + Send + Sync>,
        poll_interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let _ = Self::run_once(&mgr, executor.as_ref()).await;
            }
        })
    }
}

impl Default for BackgroundMgr {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    struct MockExecutor {
        calls: Arc<Mutex<Vec<String>>>,
        delay: Duration,
    }

    #[async_trait]
    impl BackgroundTaskExecutor for MockExecutor {
        async fn execute(&self, task: BackgroundTask) -> Result<(), String> {
            if !self.delay.is_zero() {
                sleep(self.delay).await;
            }
            let mut calls = self
                .calls
                .lock()
                .map_err(|e| format!("calls lock poisoned: {}", e))?;
            calls.push(format!("{:?}:{}", task.kind, task.path));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_push_front_back_and_dedup_merge() {
        let mut mgr = BackgroundMgr::new();
        mgr.push_back(BackgroundTaskKind::FinalizeDir, "/a");
        mgr.push_back(BackgroundTaskKind::FinalizeDir, "/b");

        // Duplicate task should be merged, and push_front should promote priority.
        mgr.push_front(BackgroundTaskKind::FinalizeDir, "/a");
        let staged = mgr.staged_tasks();
        assert_eq!(staged.len(), 2);
        assert_eq!(staged[0].path, "/a");
        assert_eq!(staged[1].path, "/b");
    }

    #[tokio::test]
    async fn test_interval_blocks_early_run_and_then_allows_execution() {
        let mgr = Arc::new(Mutex::new(BackgroundMgr::new()));
        {
            let mut guard = mgr.lock().unwrap();
            guard.set_policy(
                BackgroundTaskKind::FinalizeDir,
                BackgroundTaskPolicy::new(Duration::from_millis(80), Duration::from_secs(1)),
            );
            guard.push_back(BackgroundTaskKind::FinalizeDir, "/a");
        }

        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = MockExecutor {
            calls: calls.clone(),
            delay: Duration::from_millis(1),
        };

        // Not due yet.
        let stats1 = BackgroundMgr::run_once(&mgr, &executor).await;
        assert_eq!(stats1.executed, 0);
        assert_eq!(calls.lock().unwrap().len(), 0);

        sleep(Duration::from_millis(90)).await;
        let stats2 = BackgroundMgr::run_once(&mgr, &executor).await;
        assert_eq!(stats2.executed, 1);
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_budget_stops_taking_next_task() {
        let mgr = Arc::new(Mutex::new(BackgroundMgr::new()));
        {
            let mut guard = mgr.lock().unwrap();
            guard.set_policy(
                BackgroundTaskKind::FinalizeDir,
                BackgroundTaskPolicy::new(Duration::from_millis(1), Duration::from_millis(5)),
            );
            guard.push_back(BackgroundTaskKind::FinalizeDir, "/a");
            guard.push_back(BackgroundTaskKind::FinalizeDir, "/b");
            guard.push_back(BackgroundTaskKind::FinalizeDir, "/c");
        }

        sleep(Duration::from_millis(2)).await;

        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = MockExecutor {
            calls: calls.clone(),
            delay: Duration::from_millis(6),
        };
        let stats = BackgroundMgr::run_once(&mgr, &executor).await;

        // First task can exceed the budget, but manager should stop before next task.
        assert_eq!(stats.executed, 1);
        assert_eq!(calls.lock().unwrap().len(), 1);
    }
}
