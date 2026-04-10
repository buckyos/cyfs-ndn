//! Background outbox sender: drains `edge_outbox` entries and delivers them
//! to the target bucket's `apply_edge` endpoint.
//!
//! In a single-bucket P0 deployment, the sender loops back to the same
//! `NamedStore` instance. For multi-bucket setups, it would route via
//! the `NamedStoreMgr` routing layer.

use crate::gc_types::OutboxEntry;
use crate::named_store::NamedStore;
use log::{debug, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Configuration for the outbox sender.
pub struct OutboxSenderConfig {
    /// How many entries to fetch per poll.
    pub batch_size: usize,
    /// Interval between polls when the outbox was empty.
    pub idle_poll_interval: Duration,
    /// Interval between polls when the outbox had entries.
    pub busy_poll_interval: Duration,
}

impl Default for OutboxSenderConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            idle_poll_interval: Duration::from_secs(5),
            busy_poll_interval: Duration::from_millis(50),
        }
    }
}

/// Resolver trait: given an outbox entry, route it to the correct bucket
/// and call `apply_edge`. For P0 single-bucket, this just loops back.
#[async_trait::async_trait]
pub trait EdgeRouter: Send + Sync {
    async fn deliver(&self, entry: &OutboxEntry) -> Result<(), String>;
}

/// Loopback router: delivers edges to the same store.
pub struct LoopbackRouter {
    store: NamedStore,
}

impl LoopbackRouter {
    pub fn new(store: NamedStore) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl EdgeRouter for LoopbackRouter {
    async fn deliver(&self, entry: &OutboxEntry) -> Result<(), String> {
        self.store
            .apply_edge(entry.msg.clone())
            .await
            .map_err(|e| e.to_string())
    }
}

/// The outbox sender background task handle.
pub struct OutboxSender {
    cancel: Arc<Notify>,
}

impl OutboxSender {
    /// Spawn the sender as a background tokio task. Returns a handle that can stop it.
    pub fn spawn(
        store: NamedStore,
        router: Arc<dyn EdgeRouter>,
        config: OutboxSenderConfig,
    ) -> Self {
        let cancel = Arc::new(Notify::new());
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            Self::run_loop(store, router, config, cancel_clone).await;
        });

        Self { cancel }
    }

    /// Stop the sender.
    pub fn stop(&self) {
        self.cancel.notify_one();
    }

    async fn run_loop(
        store: NamedStore,
        router: Arc<dyn EdgeRouter>,
        config: OutboxSenderConfig,
        cancel: Arc<Notify>,
    ) {
        loop {
            let entries = match store.fetch_outbox_ready(config.batch_size).await {
                Ok(e) => e,
                Err(err) => {
                    warn!("outbox_sender: fetch failed: {}", err);
                    tokio::select! {
                        _ = tokio::time::sleep(config.idle_poll_interval) => {},
                        _ = cancel.notified() => return,
                    }
                    continue;
                }
            };

            if entries.is_empty() {
                tokio::select! {
                    _ = tokio::time::sleep(config.idle_poll_interval) => {},
                    _ = cancel.notified() => return,
                }
                continue;
            }

            for entry in &entries {
                match router.deliver(entry).await {
                    Ok(()) => {
                        if let Err(e) = store.complete_outbox_entry(entry.seq).await {
                            warn!("outbox_sender: complete failed seq={}: {}", entry.seq, e);
                        }
                    }
                    Err(e) => {
                        debug!(
                            "outbox_sender: deliver failed seq={}: {}, will retry",
                            entry.seq, e
                        );
                        if let Err(e2) = store.retry_outbox_entry(entry.seq).await {
                            warn!("outbox_sender: retry bump failed seq={}: {}", entry.seq, e2);
                        }
                    }
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(config.busy_poll_interval) => {},
                _ = cancel.notified() => return,
            }
        }
    }
}
