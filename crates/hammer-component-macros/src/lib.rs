use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Error, ExprPath, Ident, Item, LitStr, Result, Token, Type, parenthesized, parse_macro_input,
    spanned::Spanned,
};

#[derive(Clone, Copy)]
enum ComponentKind {
    Outbound,
    Inbound,
    Endpoint,
    DnsTransport,
    Router,
    Matcher,
    Probe,
    Event,
}

impl ComponentKind {
    fn parse(ident: &Ident) -> Result<Self> {
        match ident.to_string().as_str() {
            "outbound" => Ok(Self::Outbound),
            "inbound" => Ok(Self::Inbound),
            "endpoint" => Ok(Self::Endpoint),
            "dns_transport" => Ok(Self::DnsTransport),
            "router" => Ok(Self::Router),
            "matcher" => Ok(Self::Matcher),
            "probe" => Ok(Self::Probe),
            "event" => Ok(Self::Event),
            other => Err(Error::new(
                ident.span(),
                format!(
                    "unknown component kind `{other}`; expected outbound, inbound, endpoint, dns_transport, router, matcher, probe, or event"
                ),
            )),
        }
    }

    fn trait_path(self) -> TokenStream2 {
        match self {
            Self::Outbound => quote!(crate::component_registry::OutboundComponentDeclaration),
            Self::Inbound => quote!(crate::component_registry::InboundComponentDeclaration),
            Self::Endpoint => quote!(crate::component_registry::EndpointComponentDeclaration),
            Self::DnsTransport => {
                quote!(crate::component_registry::DnsTransportComponentDeclaration)
            }
            Self::Router => quote!(crate::component_registry::RouterComponentDeclaration),
            Self::Matcher => quote!(crate::component_registry::RouteMatcherComponentDeclaration),
            Self::Probe => quote!(crate::component_registry::ProbeComponentDeclaration),
            Self::Event => quote!(crate::component_registry::EventSubscriberComponentDeclaration),
        }
    }

    fn has_instance_metadata(self) -> bool {
        matches!(
            self,
            Self::Outbound | Self::Inbound | Self::Endpoint | Self::DnsTransport
        )
    }

    fn has_network_metadata(self) -> bool {
        matches!(self, Self::Outbound | Self::Endpoint)
    }

    fn has_dependency_metadata(self) -> bool {
        matches!(self, Self::Outbound | Self::Endpoint | Self::DnsTransport)
    }

    fn kind_name(self) -> &'static str {
        match self {
            Self::Outbound => "outbound",
            Self::Inbound => "inbound",
            Self::Endpoint => "endpoint",
            Self::DnsTransport => "dns_transport",
            Self::Router => "router",
            Self::Matcher => "matcher",
            Self::Probe => "probe",
            Self::Event => "event",
        }
    }
}

struct ComponentArgs {
    kind: ComponentKind,
    name: LitStr,
    builder: ExprPath,
    id: Option<Ident>,
    networks: Option<Ident>,
    dependencies: Option<Ident>,
    metrics: Option<(LitStr, LitStr)>,
    runtime: Option<Type>,
}

impl Parse for ComponentArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let kind_ident: Ident = input.parse()?;
        let kind = ComponentKind::parse(&kind_ident)?;

        let mut name = None;
        let mut builder = None;
        let mut id = None;
        let mut networks = None;
        let mut dependencies = None;
        let mut metrics = None;
        let mut runtime = None;
        while input.parse::<Option<Token![,]>>()?.is_some() {
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(Error::new(key.span(), "duplicate `name` argument"));
                    }
                    name = Some(input.parse()?);
                }
                "builder" => {
                    if builder.is_some() {
                        return Err(Error::new(key.span(), "duplicate `builder` argument"));
                    }
                    builder = Some(input.parse()?);
                }
                "id" => {
                    if id.is_some() {
                        return Err(Error::new(key.span(), "duplicate `id` argument"));
                    }
                    id = Some(input.parse()?);
                }
                "networks" => {
                    if networks.is_some() {
                        return Err(Error::new(key.span(), "duplicate `networks` argument"));
                    }
                    networks = Some(input.parse()?);
                }
                "dependencies" => {
                    if dependencies.is_some() {
                        return Err(Error::new(key.span(), "duplicate `dependencies` argument"));
                    }
                    dependencies = Some(input.parse()?);
                }
                "metrics" => {
                    if metrics.is_some() {
                        return Err(Error::new(key.span(), "duplicate `metrics` argument"));
                    }
                    let content;
                    parenthesized!(content in input);
                    let module: LitStr = content.parse()?;
                    content.parse::<Token![,]>()?;
                    let component_type: LitStr = content.parse()?;
                    metrics = Some((module, component_type));
                }
                "runtime" => {
                    if runtime.is_some() {
                        return Err(Error::new(key.span(), "duplicate `runtime` argument"));
                    }
                    runtime = Some(input.parse()?);
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown argument `{other}`; expected `name`, `builder`, `id`, `networks`, `dependencies`, `metrics`, or `runtime`"
                        ),
                    ));
                }
            }
        }

        let name = name.ok_or_else(|| Error::new(Span::call_site(), "missing `name` argument"))?;
        let builder =
            builder.ok_or_else(|| Error::new(Span::call_site(), "missing `builder` argument"))?;

        Ok(Self {
            kind,
            name,
            builder,
            id,
            networks,
            dependencies,
            metrics,
            runtime,
        })
    }
}

/// Marks a runtime component type with its config name and builder function.
///
/// Example:
///
/// ```ignore
/// #[hammer_component_macros::hammer_component(outbound, name = "direct", builder = build_outbound)]
/// pub struct DirectOutbound { ... }
/// ```
#[proc_macro_attribute]
pub fn hammer_component(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ComponentArgs);
    let item = parse_macro_input!(input as Item);

    let ident = match &item {
        Item::Struct(item) => &item.ident,
        Item::Enum(item) => &item.ident,
        _ => {
            return Error::new(
                item.span(),
                "`hammer_component` can only be attached to a struct or enum",
            )
            .to_compile_error()
            .into();
        }
    };
    let generics = match &item {
        Item::Struct(item) => &item.generics,
        Item::Enum(item) => &item.generics,
        _ => unreachable!(),
    };

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let trait_path = args.kind.trait_path();
    let kind = args.kind;
    let kind_name = LitStr::new(kind.kind_name(), Span::call_site());
    let name = args.name.clone();
    let meta_name = args.name.clone();
    let builder = args.builder;
    let id = args
        .id
        .unwrap_or_else(|| Ident::new("id", Span::call_site()));
    let networks = args
        .networks
        .unwrap_or_else(|| Ident::new("networks", Span::call_site()));
    let dependencies = args
        .dependencies
        .unwrap_or_else(|| Ident::new("dependencies", Span::call_site()));

    let id_value = if kind.has_instance_metadata() {
        quote!(self.#id.clone())
    } else {
        quote!(#meta_name.to_owned())
    };
    let networks_value = if kind.has_network_metadata() {
        quote!(self.#networks.clone())
    } else {
        quote!(Vec::new())
    };
    let dependencies_value = if kind.has_dependency_metadata() {
        quote!(self.#dependencies.clone())
    } else {
        quote!(Vec::new())
    };
    let metrics_value = if let Some((module, component_type)) = args.metrics {
        quote!(Some(::hammer_adapter::ComponentMetricsMeta {
            module: #module,
            component_type: #component_type,
        }))
    } else {
        quote!(None)
    };
    let has_runtime_override = args.runtime.is_some();
    let declaration_ty = args
        .runtime
        .clone()
        .map(|ty| quote!(#ty))
        .unwrap_or_else(|| quote!(#ident #ty_generics));
    let declaration_impl_head = if has_runtime_override {
        quote!(impl #trait_path for #declaration_ty)
    } else {
        quote!(impl #impl_generics #trait_path for #declaration_ty #where_clause)
    };

    let declaration_impl = match kind {
        ComponentKind::Outbound => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build(
                    logger: ::hammer_core::log::Logger,
                    id: String,
                    kind: &::hammer_core::config::OutboundKind,
                    protector: crate::socket_protector::SocketProtector,
                    control_handle: Option<::std::sync::Arc<crate::ControlThreadHandle>>,
                ) -> ::hammer_core::error::HammerResult<::hammer_adapter::outbound::OutboundComponent> {
                    let runtime: ::std::sync::Arc<#declaration_ty> = #builder(logger, id, kind, protector, control_handle)?;
                    let meta = ::hammer_adapter::ComponentMetadata::component_meta(runtime.as_ref());
                    let runtime: ::std::sync::Arc<dyn ::hammer_adapter::Outbound> = runtime;
                    Ok(::hammer_adapter::RuntimeComponent::new(meta, runtime))
                }
            }
        },
        ComponentKind::Inbound => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                #[allow(clippy::too_many_arguments)]
                fn build(
                    id: String,
                    logger: ::hammer_core::log::Logger,
                    kind: &::hammer_core::config::InboundKind,
                    router: ::std::sync::Arc<crate::Router>,
                    dns_router: Option<::std::sync::Arc<crate::inbounds::RuntimeDnsRouter>>,
                    outbound: Option<::std::sync::Arc<crate::OutboundManager>>,
                    platform: Option<::std::sync::Arc<dyn ::hammer_adapter::PlatformInterface>>,
                    metrics: ::std::sync::Arc<::hammer_core::metrics::MetricsRegistry>,
                ) -> ::hammer_core::error::HammerResult<::hammer_adapter::inbound::InboundComponent> {
                    let runtime: ::std::sync::Arc<#declaration_ty> = #builder(
                        id, logger, kind, router, dns_router, outbound, platform, metrics
                    )?;
                    let meta = ::hammer_adapter::ComponentMetadata::component_meta(runtime.as_ref());
                    let runtime: ::std::sync::Arc<dyn ::hammer_adapter::Inbound> = runtime;
                    Ok(::hammer_adapter::RuntimeComponent::new(meta, runtime))
                }
            }
        },
        ComponentKind::Endpoint => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build(
                    logger: ::hammer_core::log::Logger,
                    option: &::hammer_core::config::Endpoint,
                    platform: Option<::std::sync::Arc<dyn ::hammer_adapter::PlatformInterface>>,
                    control_handle: Option<::std::sync::Arc<crate::ControlThreadHandle>>,
                ) -> ::hammer_core::error::HammerResult<::hammer_adapter::EndpointComponent> {
                    let runtime: ::std::sync::Arc<#declaration_ty> = #builder(logger, option, platform, control_handle)?;
                    let meta = ::hammer_adapter::ComponentMetadata::component_meta(runtime.as_ref());
                    let endpoint: ::std::sync::Arc<dyn ::hammer_adapter::Endpoint> = runtime;
                    Ok(::hammer_adapter::RuntimeComponent::new(meta, endpoint))
                }
            }
        },
        ComponentKind::DnsTransport => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build(
                    id: String,
                    kind: &::hammer_core::config::DnsServerKind,
                    logger: ::hammer_core::log::Logger,
                    outbound: Option<::std::sync::Arc<crate::OutboundManager>>,
                    bootstrap: Option<::hammer_adapter::dns::DnsTransportComponent>,
                    protector: crate::socket_protector::SocketProtector,
                ) -> ::hammer_core::error::HammerResult<::hammer_adapter::dns::DnsTransportComponent> {
                    let runtime: ::std::sync::Arc<#declaration_ty> = #builder(
                        id, kind, logger, outbound, bootstrap, protector
                    )?;
                    let meta = ::hammer_adapter::ComponentMetadata::component_meta(runtime.as_ref());
                    let runtime: ::std::sync::Arc<dyn ::hammer_adapter::DnsTransport> = runtime;
                    Ok(::hammer_adapter::RuntimeComponent::new(meta, runtime))
                }
            }
        },
        ComponentKind::Router => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build(
                    logger: ::hammer_core::log::Logger,
                    options: ::hammer_core::config::RouteOptions,
                    outbound: ::std::sync::Arc<crate::OutboundManager>,
                    metrics: ::std::sync::Arc<::hammer_core::metrics::MetricsRegistry>,
                ) -> ::hammer_core::error::HammerResult<crate::Router> {
                    #builder(logger, options, outbound, metrics)
                }
            }
        },
        ComponentKind::Matcher => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build(
                    matcher: ::hammer_core::config::RuleMatcher,
                ) -> ::hammer_core::error::HammerResult<crate::route::RuntimeMatcher> {
                    #builder(matcher)
                }
            }
        },
        ComponentKind::Probe => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build() -> ::hammer_adapter::probe::ProbeProtocolComponent {
                    let runtime: ::std::sync::Arc<#declaration_ty> = #builder();
                    let meta = ::hammer_adapter::ComponentMetadata::component_meta(runtime.as_ref());
                    let runtime: ::std::sync::Arc<dyn ::hammer_adapter::ProbeProtocol> = runtime;
                    ::hammer_adapter::RuntimeComponent::new(meta, runtime)
                }
            }
        },
        ComponentKind::Event => quote! {
            #declaration_impl_head {
                const TYPE_NAME: &'static str = #name;

                fn build(
                    logger: ::hammer_core::log::Logger,
                    control_handle: ::std::sync::Arc<crate::ControlThreadHandle>,
                ) -> ::hammer_core::error::HammerResult<::std::vec::Vec<crate::ControlEventSubscriptionHandle>> {
                    #builder(logger, control_handle)
                }
            }
        },
    };

    quote! {
        #item

        impl #impl_generics ::hammer_adapter::ComponentMetadata for #ident #ty_generics #where_clause {
            fn component_meta(&self) -> ::hammer_adapter::ComponentMeta {
                ::hammer_adapter::ComponentMeta::new(
                    #kind_name,
                    #meta_name,
                    #id_value,
                    #networks_value,
                    #dependencies_value,
                    #metrics_value,
                )
            }
        }

        #declaration_impl
    }
    .into()
}
