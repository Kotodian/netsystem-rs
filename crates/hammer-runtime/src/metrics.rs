use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKind {
    Counter,
    Gauge,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
        }
    }
}

impl fmt::Display for MetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricLabel {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSample {
    pub module: String,
    pub component_type: String,
    pub component_id: String,
    pub name: String,
    pub kind: MetricKind,
    pub value: u64,
    pub labels: Vec<MetricLabel>,
}

#[derive(Debug, Default)]
pub struct MetricsRegistry {
    values: Mutex<HashMap<MetricKey, Arc<AtomicU64>>>,
}

impl MetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn scope(
        self: &Arc<Self>,
        module: impl Into<String>,
        component_type: impl Into<String>,
        component_id: impl Into<String>,
    ) -> MetricsScope {
        MetricsScope {
            registry: Arc::clone(self),
            module: module.into(),
            component_type: component_type.into(),
            component_id: component_id.into(),
        }
    }

    fn register(&self, key: MetricKey) -> Arc<AtomicU64> {
        let mut values = self.values.lock().expect("MetricsRegistry poisoned");
        Arc::clone(
            values
                .entry(key)
                .or_insert_with(|| Arc::new(AtomicU64::new(0))),
        )
    }

    pub fn snapshot(&self) -> Vec<MetricSample> {
        let mut samples: Vec<_> = self
            .values
            .lock()
            .expect("MetricsRegistry poisoned")
            .iter()
            .map(|(key, value)| MetricSample {
                module: key.module.clone(),
                component_type: key.component_type.clone(),
                component_id: key.component_id.clone(),
                name: key.name.clone(),
                kind: key.kind,
                value: value.load(Ordering::Relaxed),
                labels: key
                    .labels
                    .iter()
                    .map(|(key, value)| MetricLabel {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            })
            .collect();
        samples.sort_by(|a, b| {
            (
                &a.module,
                &a.component_type,
                &a.component_id,
                &a.name,
                a.kind.as_str(),
                labels_sort_key(&a.labels),
            )
                .cmp(&(
                    &b.module,
                    &b.component_type,
                    &b.component_id,
                    &b.name,
                    b.kind.as_str(),
                    labels_sort_key(&b.labels),
                ))
        });
        samples
    }
}

#[derive(Debug, Clone)]
pub struct MetricsScope {
    registry: Arc<MetricsRegistry>,
    module: String,
    component_type: String,
    component_id: String,
}

impl MetricsScope {
    pub fn recorder(&self) -> RegistryRecorder {
        RegistryRecorder::new(
            Arc::clone(&self.registry),
            self.module.clone(),
            self.component_type.clone(),
            self.component_id.clone(),
        )
    }

    pub fn child(
        &self,
        component_type: impl Into<String>,
        component_id: impl Into<String>,
    ) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            module: self.module.clone(),
            component_type: component_type.into(),
            component_id: component_id.into(),
        }
    }

    pub fn counter(&self, name: &str) -> MetricCounter {
        self.counter_with_labels(name, std::iter::empty::<(&str, &str)>())
    }

    pub fn counter_with_labels<I, K, V>(&self, name: &str, labels: I) -> MetricCounter
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        MetricCounter {
            value: self.register(name, MetricKind::Counter, labels),
        }
    }

    pub fn gauge(&self, name: &str) -> MetricGauge {
        self.gauge_with_labels(name, std::iter::empty::<(&str, &str)>())
    }

    pub fn gauge_with_labels<I, K, V>(&self, name: &str, labels: I) -> MetricGauge
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        MetricGauge {
            value: self.register(name, MetricKind::Gauge, labels),
        }
    }

    fn register<I, K, V>(&self, name: &str, kind: MetricKind, labels: I) -> Arc<AtomicU64>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.registry.register(MetricKey::new(
            &self.module,
            &self.component_type,
            &self.component_id,
            name,
            kind,
            labels,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct MetricCounter {
    value: Arc<AtomicU64>,
}

impl MetricCounter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub struct MetricGauge {
    value: Arc<AtomicU64>,
}

impl MetricGauge {
    pub fn set(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        saturating_fetch_sub(&self.value, 1);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricKey {
    module: String,
    component_type: String,
    component_id: String,
    name: String,
    kind: MetricKind,
    labels: Vec<(String, String)>,
}

impl MetricKey {
    fn new<I, K, V>(
        module: &str,
        component_type: &str,
        component_id: &str,
        name: &str,
        kind: MetricKind,
        labels: I,
    ) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut labels: Vec<_> = labels
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        labels.sort();
        labels.dedup();
        Self {
            module: module.to_owned(),
            component_type: component_type.to_owned(),
            component_id: component_id.to_owned(),
            name: name.to_owned(),
            kind,
            labels,
        }
    }
}

fn labels_sort_key(labels: &[MetricLabel]) -> String {
    labels
        .iter()
        .map(|label| format!("{}={}", label.key, label.value))
        .collect::<Vec<_>>()
        .join(",")
}

/// Bridges the `metrics` crate's [`metrics::Recorder`] trait into
/// [`MetricsRegistry`].
///
/// Install once per process via [`metrics::set_global_recorder`]; afterwards
/// every `metrics::counter!()` / `metrics::gauge!()` call (including the
/// ones emitted by `metrics-derive`) lands in the same backing storage that
/// [`MetricsRegistry::snapshot`] reads, so snapshot and log-dump paths keep
/// working unchanged.
///
/// Each recorder pins a single `(module, component_type, component_id)`
/// scope; the metric name and any labels supplied via `metrics::Key` are
/// translated into [`MetricKey`] entries beneath that scope. Histograms
/// are not used by this codebase and are wired to no-ops.
pub struct RegistryRecorder {
    registry: Arc<MetricsRegistry>,
    module: String,
    component_type: String,
    component_id: String,
}

impl RegistryRecorder {
    pub fn new(
        registry: Arc<MetricsRegistry>,
        module: impl Into<String>,
        component_type: impl Into<String>,
        component_id: impl Into<String>,
    ) -> Self {
        Self {
            registry,
            module: module.into(),
            component_type: component_type.into(),
            component_id: component_id.into(),
        }
    }

    fn metric_key_from(&self, key: &metrics::Key, kind: MetricKind) -> MetricKey {
        let labels = key
            .labels()
            .map(|l| (l.key().to_owned(), l.value().to_owned()));
        MetricKey::new(
            &self.module,
            &self.component_type,
            &self.component_id,
            key.name(),
            kind,
            labels,
        )
    }
}

impl metrics::Recorder for RegistryRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let arc = self
            .registry
            .register(self.metric_key_from(key, MetricKind::Counter));
        metrics::Counter::from_arc(Arc::new(AtomicCounterFn(arc)))
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        let arc = self
            .registry
            .register(self.metric_key_from(key, MetricKind::Gauge));
        metrics::Gauge::from_arc(Arc::new(AtomicGaugeFn(arc)))
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

struct AtomicCounterFn(Arc<AtomicU64>);

impl metrics::CounterFn for AtomicCounterFn {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        // metrics::CounterFn::absolute is "set to at least this value"
        // tolerant to reordering by external sources. CAS until the
        // observed value catches up to `value`.
        let mut current = self.0.load(Ordering::Relaxed);
        while value > current {
            match self
                .0
                .compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

struct AtomicGaugeFn(Arc<AtomicU64>);

impl AtomicGaugeFn {
    fn clamp_u64(value: f64) -> u64 {
        if value.is_nan() || value <= 0.0 {
            0
        } else if value >= u64::MAX as f64 {
            u64::MAX
        } else {
            value as u64
        }
    }
}

impl metrics::GaugeFn for AtomicGaugeFn {
    fn increment(&self, value: f64) {
        self.0.fetch_add(Self::clamp_u64(value), Ordering::Relaxed);
    }

    fn decrement(&self, value: f64) {
        saturating_fetch_sub(&self.0, Self::clamp_u64(value));
    }

    fn set(&self, value: f64) {
        self.0.store(Self::clamp_u64(value), Ordering::Relaxed);
    }
}

fn saturating_fetch_sub(value: &AtomicU64, amount: u64) {
    let mut current = value.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(amount);
        match value.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}
