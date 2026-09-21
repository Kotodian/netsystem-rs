use std::sync::OnceLock;

use hammer_app::{AppNamespace, AppNamespaceMain};
use hammer_plugin_ip::{IpVersion, fib_table_find, fib_table_get_index_for_sw_if_index};
use hammer_service::net::{FibEntrySourceBehaviorId, FibSource, NetMain};

use crate::api::AppNamespaceAddDelRetval;
use crate::{IpSessionFamily, session_lookup};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNamespaceBinding {
    sw_if_index: u32,
    ip4_fib_index: u32,
    ip6_fib_index: u32,
    local_table_index: u32,
}

impl IpNamespaceBinding {
    #[inline(always)]
    pub const fn new(
        sw_if_index: u32,
        ip4_fib_index: u32,
        ip6_fib_index: u32,
        local_table_index: u32,
    ) -> Self {
        Self {
            sw_if_index,
            ip4_fib_index,
            ip6_fib_index,
            local_table_index,
        }
    }

    #[inline(always)]
    pub const fn sw_if_index(&self) -> u32 {
        self.sw_if_index
    }

    #[inline(always)]
    pub const fn fib_index(&self, family: IpSessionFamily) -> u32 {
        match family {
            IpSessionFamily::Ip4 => self.ip4_fib_index,
            IpSessionFamily::Ip6 => self.ip6_fib_index,
        }
    }

    #[inline(always)]
    pub const fn local_table_index(&self) -> u32 {
        self.local_table_index
    }

    pub fn fibs(&self) -> impl Iterator<Item = (IpSessionFamily, u32)> + '_ {
        [
            (IpSessionFamily::Ip4, self.ip4_fib_index),
            (IpSessionFamily::Ip6, self.ip6_fib_index),
        ]
        .into_iter()
        .filter(|(_, fib_index)| *fib_index != u32::MAX)
    }
}

pub struct IpNamespaceMain {
    namespaces: AppNamespaceMain<IpNamespaceBinding>,
    fib_source: FibSource,
}

impl IpNamespaceMain {
    fn new(fib_source: FibSource) -> Self {
        Self {
            namespaces: AppNamespaceMain::new(),
            fib_source,
        }
    }

    #[inline]
    pub fn get(&self, index: u32) -> Option<&AppNamespace<IpNamespaceBinding>> {
        self.namespaces.get(index)
    }

    #[inline]
    pub fn find(&self, id: &str) -> Option<(u32, &AppNamespace<IpNamespaceBinding>)> {
        self.namespaces.find(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, &AppNamespace<IpNamespaceBinding>)> + '_ {
        self.namespaces.iter()
    }

    pub fn add_or_rebind(
        &self,
        id: String,
        secret: u64,
        sw_if_index: u32,
        ip4_fib_id: u32,
        ip6_fib_id: u32,
    ) -> Result<u32, AppNamespaceAddDelRetval> {
        let (ip4_fib_index, ip6_fib_index) = resolve_fibs(sw_if_index, ip4_fib_id, ip6_fib_id)?;
        let lookup = session_lookup();
        if let Some((appns_index, namespace)) = self.namespaces.find(&id) {
            let previous = *namespace.binding();
            let replacement = IpNamespaceBinding::new(
                sw_if_index,
                ip4_fib_index,
                ip6_fib_index,
                previous.local_table_index(),
            );
            bind_changed_fibs(lookup, appns_index, previous, replacement, self.fib_source);
            self.namespaces.replace(appns_index, secret, replacement);
            unbind_changed_fibs(lookup, appns_index, previous, replacement, self.fib_source);
            return Ok(appns_index);
        }

        let local_table_index = lookup.alloc_local();
        let binding =
            IpNamespaceBinding::new(sw_if_index, ip4_fib_index, ip6_fib_index, local_table_index);
        let appns_index = self.namespaces.insert(id, secret, binding);
        lookup.bind_local(appns_index, local_table_index);
        for (family, fib_index) in binding.fibs() {
            lookup.bind_global(appns_index, family, fib_index, self.fib_source);
        }
        Ok(appns_index)
    }

    pub fn delete(&self, id: &str) -> Result<(), AppNamespaceAddDelRetval> {
        let (appns_index, namespace) = self
            .namespaces
            .find(id)
            .ok_or(AppNamespaceAddDelRetval::Invalid)?;
        let binding = *namespace.binding();
        let lookup = session_lookup();
        for (family, fib_index) in binding.fibs() {
            lookup.unbind_global(appns_index, family, fib_index, self.fib_source);
        }
        lookup.free_local(appns_index, binding.local_table_index());
        self.namespaces
            .remove(id)
            .expect("validated Application Namespace remains live until removal");
        Ok(())
    }
}

fn resolve_fibs(
    sw_if_index: u32,
    ip4_fib_id: u32,
    ip6_fib_id: u32,
) -> Result<(u32, u32), AppNamespaceAddDelRetval> {
    let (ip4_fib_index, ip6_fib_index) = if sw_if_index != u32::MAX {
        if NetMain::global()
            .expect("network Main is initialized before namespace operations")
            .interface_main()
            .software_interface(sw_if_index)
            .is_none()
        {
            return Err(AppNamespaceAddDelRetval::Invalid);
        }
        (
            fib_table_get_index_for_sw_if_index(IpVersion::V4, sw_if_index).unwrap_or(u32::MAX),
            fib_table_get_index_for_sw_if_index(IpVersion::V6, sw_if_index).unwrap_or(u32::MAX),
        )
    } else {
        (
            resolve_fib_id(IpVersion::V4, ip4_fib_id)?,
            resolve_fib_id(IpVersion::V6, ip6_fib_id)?,
        )
    };
    if ip4_fib_index == u32::MAX && ip6_fib_index == u32::MAX {
        return Err(AppNamespaceAddDelRetval::Invalid);
    }
    Ok((ip4_fib_index, ip6_fib_index))
}

fn resolve_fib_id(version: IpVersion, fib_id: u32) -> Result<u32, AppNamespaceAddDelRetval> {
    if fib_id == u32::MAX {
        return Ok(u32::MAX);
    }
    fib_table_find(version, fib_id).ok_or(AppNamespaceAddDelRetval::Invalid)
}

fn bind_changed_fibs(
    lookup: &crate::IpSessionLookup,
    appns_index: u32,
    previous: IpNamespaceBinding,
    replacement: IpNamespaceBinding,
    source: FibSource,
) {
    for family in [IpSessionFamily::Ip4, IpSessionFamily::Ip6] {
        let previous_index = previous.fib_index(family);
        let replacement_index = replacement.fib_index(family);
        if replacement_index != u32::MAX && replacement_index != previous_index {
            lookup.bind_global(appns_index, family, replacement_index, source);
        }
    }
}

fn unbind_changed_fibs(
    lookup: &crate::IpSessionLookup,
    appns_index: u32,
    previous: IpNamespaceBinding,
    replacement: IpNamespaceBinding,
    source: FibSource,
) {
    for family in [IpSessionFamily::Ip4, IpSessionFamily::Ip6] {
        let previous_index = previous.fib_index(family);
        if previous_index != u32::MAX && previous_index != replacement.fib_index(family) {
            lookup.unbind_global(appns_index, family, previous_index, source);
        }
    }
}

#[derive(hammer_component_macros::FibSource)]
#[fib_source(
    name = "session",
    priority = 0x80,
    behavior = FibEntrySourceBehaviorId::SIMPLE
)]
struct SessionFibSource;

static NAMESPACES: OnceLock<IpNamespaceMain> = OnceLock::new();

#[hammer_component_macros::init_function(
    name = "ip_namespace_init",
    runs_after = ["session_lookup_init", "ip_lookup_init", "net_main_init"]
)]
fn init_ip_namespace() -> hammer_runtime::RuntimeResult<()> {
    let net = NetMain::global()?;
    let fib_source = SessionFibSource::register_fib_source(&mut net.fib_sources_mut());
    let main = IpNamespaceMain::new(fib_source);
    let lookup = session_lookup();
    let local_table_index = lookup.alloc_local();
    let binding = IpNamespaceBinding::new(u32::MAX, 0, 0, local_table_index);
    let appns_index = main.namespaces.insert("default".to_owned(), 0, binding);
    assert_eq!(
        appns_index, 0,
        "default Application Namespace owns index zero"
    );
    lookup.bind_local(appns_index, local_table_index);
    lookup.bind_global(appns_index, IpSessionFamily::Ip4, 0, fib_source);
    lookup.bind_global(appns_index, IpSessionFamily::Ip6, 0, fib_source);
    assert!(
        NAMESPACES.set(main).is_ok(),
        "IP Namespace initialization callback executes once"
    );
    Ok(())
}

#[inline]
pub fn namespaces() -> &'static IpNamespaceMain {
    NAMESPACES
        .get()
        .expect("IP Namespace Main is initialized before use")
}
