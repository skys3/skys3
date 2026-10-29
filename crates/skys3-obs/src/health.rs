//! Liveness and readiness state for the admin listener's health endpoints.
//!
//! Liveness (`/healthz`) means only that the process is serving HTTP.
//! Readiness (`/readyz`) means every registered component is ready to
//! serve: a component registers with [`Health::register`] and reports
//! through the returned [`Readiness`] handle. For example, startup recovery
//! registers as not ready and turns ready once the logs have been replayed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The readiness of a node, as the sum of its registered components.
///
/// Cloning is cheap, and clones share the same state. A node with no
/// registered components is ready.
#[derive(Clone, Debug, Default)]
pub struct Health {
    components: Arc<Mutex<BTreeMap<String, bool>>>,
}

impl Health {
    /// Creates a health state with no components, which is ready.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a component that starts out not ready.
    ///
    /// # Panics
    ///
    /// Panics if a component with the same name is already registered. Two
    /// components under one name would hide each other's state.
    #[must_use = "the component stays not ready until its handle reports it ready"]
    pub fn register(&self, component: impl Into<String>) -> Readiness {
        let component = component.into();
        let previous = self.lock().insert(component.clone(), false);
        assert!(
            previous.is_none(),
            "health component {component:?} is already registered"
        );
        Readiness {
            health: self.clone(),
            component,
        }
    }

    /// Returns whether every registered component is ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lock().values().all(|ready| *ready)
    }

    /// Returns the names of the components that are not ready, in order.
    #[must_use]
    pub fn not_ready(&self) -> Vec<String> {
        self.lock()
            .iter()
            .filter(|(_, ready)| !**ready)
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, bool>> {
        // Every critical section is a single map operation, so a poisoned
        // lock still guards a consistent map.
        self.components
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A component's handle for reporting its readiness.
///
/// Dropping the handle keeps the component's last reported state, so a
/// component that fails without reporting stays not ready.
#[derive(Debug)]
pub struct Readiness {
    health: Health,
    component: String,
}

impl Readiness {
    /// Reports whether the component is ready.
    pub fn set_ready(&self, ready: bool) {
        self.health.lock().insert(self.component.clone(), ready);
    }

    /// Returns the component's name.
    #[must_use]
    pub fn component(&self) -> &str {
        &self.component
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_health_is_ready() {
        let health = Health::new();
        assert!(health.is_ready());
        assert!(health.not_ready().is_empty());
    }

    #[test]
    fn ready_only_when_every_component_is_ready() {
        let health = Health::new();
        let recovery = health.register("recovery");
        let control = health.register("control_store");
        assert_eq!(recovery.component(), "recovery");
        assert!(!health.is_ready());
        assert_eq!(health.not_ready(), ["control_store", "recovery"]);

        recovery.set_ready(true);
        assert_eq!(health.not_ready(), ["control_store"]);
        control.set_ready(true);
        assert!(health.is_ready());

        control.set_ready(false);
        assert!(!health.is_ready());
    }

    #[test]
    fn dropped_handle_keeps_its_last_state() {
        let health = Health::new();
        drop(health.register("recovery"));
        assert_eq!(health.not_ready(), ["recovery"]);
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn duplicate_component_panics() {
        let health = Health::new();
        let _first = health.register("recovery");
        let _second = health.register("recovery");
    }
}
