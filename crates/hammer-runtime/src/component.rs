use hammer_core::Network;
use hammer_infra::vec::Vec;

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

pub trait ComponentMetadata {
    fn component_meta(&self) -> ComponentMeta;
}
