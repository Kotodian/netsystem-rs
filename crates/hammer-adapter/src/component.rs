use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use crate::Network;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentMetricsMeta {
    pub module: &'static str,
    pub component_type: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentMeta {
    kind: &'static str,
    type_name: &'static str,
    id: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    metrics: Option<ComponentMetricsMeta>,
}

impl ComponentMeta {
    pub fn new(
        kind: &'static str,
        type_name: &'static str,
        id: impl Into<String>,
        networks: Vec<Network>,
        dependencies: Vec<String>,
        metrics: Option<ComponentMetricsMeta>,
    ) -> Self {
        Self {
            kind,
            type_name,
            id: id.into(),
            networks,
            dependencies,
            metrics,
        }
    }

    pub fn kind(&self) -> &'static str {
        self.kind
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn networks(&self) -> &[Network] {
        &self.networks
    }

    pub fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    pub fn metrics(&self) -> Option<ComponentMetricsMeta> {
        self.metrics
    }
}

pub struct RuntimeComponent<T: ?Sized> {
    meta: ComponentMeta,
    runtime: Arc<T>,
}

impl<T: ?Sized> RuntimeComponent<T> {
    pub fn new(meta: ComponentMeta, runtime: Arc<T>) -> Self {
        Self { meta, runtime }
    }

    pub fn meta(&self) -> &ComponentMeta {
        &self.meta
    }

    pub fn runtime(&self) -> &Arc<T> {
        &self.runtime
    }

    pub fn into_runtime(self) -> Arc<T> {
        self.runtime
    }

    pub fn kind(&self) -> &'static str {
        self.meta.kind()
    }

    pub fn type_name(&self) -> &'static str {
        self.meta.type_name()
    }

    pub fn id(&self) -> &str {
        self.meta.id()
    }

    pub fn networks(&self) -> &[Network] {
        self.meta.networks()
    }

    pub fn dependencies(&self) -> &[String] {
        self.meta.dependencies()
    }
}

impl<T: ?Sized> Clone for RuntimeComponent<T> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl<T: ?Sized> Deref for RuntimeComponent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.runtime.as_ref()
    }
}

pub trait ComponentMetadata {
    fn component_meta(&self) -> ComponentMeta;
}

pub trait AsAnyComponent {
    fn as_any(&self) -> &dyn Any;
}

impl<T> AsAnyComponent for T
where
    T: Any,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}
