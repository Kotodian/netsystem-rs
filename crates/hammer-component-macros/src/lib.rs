use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Error, Expr, ExprPath, Field, Fields, FieldsNamed, GenericParam, Generics, Ident,
    Item, ItemEnum, ItemStruct, LitStr, Path, Result, Token, Type, bracketed, parenthesized,
    parse_macro_input, parse_quote, spanned::Spanned,
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

struct FeatureArgs {
    arc: Path,
    id: Ident,
    runs_before: Vec<Ident>,
    runs_after: Vec<Ident>,
}

#[derive(Default)]
struct NodeArgs {
    next: Option<Path>,
    next_node: bool,
    sibling_of: Option<Path>,
    role: Option<NodeRole>,
    start_arc: Option<Path>,
}

#[derive(Clone, Copy)]
enum NodeRole {
    Internal,
    Driver,
}

#[derive(Default)]
struct NodeFieldArgs {
    default: Option<Expr>,
    into: bool,
}

impl Parse for FeatureArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut arc = None;
        let mut id = None;
        let mut runs_before = Vec::new();
        let mut runs_after = Vec::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "arc" => {
                    if arc.is_some() {
                        return Err(Error::new(key.span(), "duplicate `arc` argument"));
                    }
                    arc = Some(input.parse()?);
                }
                "id" => {
                    if id.is_some() {
                        return Err(Error::new(key.span(), "duplicate `id` argument"));
                    }
                    id = Some(input.parse()?);
                }
                "runs_before" => {
                    if !runs_before.is_empty() {
                        return Err(Error::new(key.span(), "duplicate `runs_before` argument"));
                    }
                    runs_before = parse_ident_array(input)?;
                }
                "runs_after" => {
                    if !runs_after.is_empty() {
                        return Err(Error::new(key.span(), "duplicate `runs_after` argument"));
                    }
                    runs_after = parse_ident_array(input)?;
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown argument `{other}`; expected `arc`, `id`, `runs_before`, or `runs_after`"
                        ),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }

        Ok(Self {
            arc: arc.ok_or_else(|| Error::new(Span::call_site(), "missing `arc` argument"))?,
            id: id.ok_or_else(|| Error::new(Span::call_site(), "missing `id` argument"))?,
            runs_before,
            runs_after,
        })
    }
}

impl Parse for NodeArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = Self::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "role" => {
                    if args.role.is_some() {
                        return Err(Error::new(key.span(), "duplicate `role` argument"));
                    }
                    input.parse::<Token![=]>()?;
                    let role: Ident = input.parse()?;
                    args.role = Some(match role.to_string().as_str() {
                        "internal" => NodeRole::Internal,
                        "driver" => NodeRole::Driver,
                        other => {
                            return Err(Error::new(
                                role.span(),
                                format!(
                                    "unknown node role `{other}`; expected `internal` or `driver`"
                                ),
                            ));
                        }
                    });
                }
                "next" => {
                    if args.next.is_some() {
                        return Err(Error::new(key.span(), "duplicate `next` argument"));
                    }
                    input.parse::<Token![=]>()?;
                    args.next = Some(input.parse()?);
                }
                "next_node" => {
                    if args.next_node {
                        return Err(Error::new(key.span(), "duplicate `next_node` argument"));
                    }
                    args.next_node = true;
                }
                "sibling_of" => {
                    if args.sibling_of.is_some() {
                        return Err(Error::new(key.span(), "duplicate `sibling_of` argument"));
                    }
                    input.parse::<Token![=]>()?;
                    args.sibling_of = Some(input.parse()?);
                }
                "start_arc" => {
                    if args.start_arc.is_some() {
                        return Err(Error::new(key.span(), "duplicate `start_arc` argument"));
                    }
                    input.parse::<Token![=]>()?;
                    args.start_arc = Some(input.parse()?);
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown argument `{other}`; expected `role`, `next`, `next_node`, `sibling_of`, or `start_arc`"
                        ),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        if args.next.is_some() && args.next_node {
            return Err(Error::new(
                Span::call_site(),
                "`next` and `next_node` are mutually exclusive",
            ));
        }
        if args.next.is_some() && args.sibling_of.is_some() {
            return Err(Error::new(
                Span::call_site(),
                "`next` and `sibling_of` are mutually exclusive",
            ));
        }
        if args.next_node && args.sibling_of.is_some() {
            return Err(Error::new(
                Span::call_site(),
                "`next_node` and `sibling_of` are mutually exclusive",
            ));
        }
        Ok(args)
    }
}

impl Parse for NodeFieldArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = Self::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "default" => {
                    if args.default.is_some() {
                        return Err(Error::new(key.span(), "duplicate `default` argument"));
                    }
                    args.default = if input.parse::<Option<Token![=]>>()?.is_some() {
                        Some(input.parse()?)
                    } else {
                        Some(parse_quote!(::std::default::Default::default()))
                    };
                }
                "into" => {
                    if args.into {
                        return Err(Error::new(key.span(), "duplicate `into` argument"));
                    }
                    args.into = true;
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!("unknown field argument `{other}`; expected `default` or `into`"),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        Ok(args)
    }
}

fn parse_ident_array(input: ParseStream<'_>) -> Result<Vec<Ident>> {
    let content;
    bracketed!(content in input);
    let mut values = Vec::new();
    while !content.is_empty() {
        values.push(content.parse()?);
        if content.parse::<Option<Token![,]>>()?.is_none() {
            break;
        }
    }
    Ok(values)
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
                    router: ::std::sync::Arc<dyn ::hammer_adapter::Router>,
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

/// Defines a dataplane node struct, its next-node storage, and its constructor.
///
/// Examples:
///
/// ```ignore
/// #[hammer_component_macros::node(next = IpInputNext, start_arc = A)]
/// pub struct IpInputNode<A: FeatureArcSpec = IpUnicastArc>;
///
/// #[hammer_component_macros::node(next = RouteMatchNext)]
/// pub struct RouteMatchNode<R> {
///     router: R,
/// }
/// ```
#[proc_macro_attribute]
pub fn node(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as NodeArgs);
    let item = parse_macro_input!(input as ItemStruct);
    expand_node(args, item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a dataplane feature arc enum.
///
/// Example:
///
/// ```ignore
/// #[hammer_component_macros::feature_arc]
/// pub enum IpUnicastArc {
///     AclInput,
/// }
/// ```
#[proc_macro_attribute]
pub fn feature_arc(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(Span::call_site(), "`feature_arc` does not accept arguments")
            .to_compile_error()
            .into();
    }
    let item = parse_macro_input!(input as ItemEnum);

    let ident = &item.ident;
    let generics = &item.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        #item

        impl #impl_generics ::hammer_service::data_plane::FeatureArcSpec
            for #ident #ty_generics #where_clause
        {}
    }
    .into()
}

/// Marks a dataplane node type as a feature in a specific feature arc.
///
/// Example:
///
/// ```ignore
/// #[hammer_component_macros::feature(arc = IpUnicastArc, id = AclInput)]
/// pub struct AclInputNode { ... }
/// ```
#[proc_macro_attribute]
pub fn feature(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as FeatureArgs);
    let item = parse_macro_input!(input as Item);

    let ident = match &item {
        Item::Struct(item) => &item.ident,
        Item::Enum(item) => &item.ident,
        _ => {
            return Error::new(
                item.span(),
                "`feature` can only be attached to a struct or enum",
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
    let arc = args.arc;
    let id = args.id;
    let runs_before = args.runs_before;
    let runs_after = args.runs_after;
    let runs_before_fn = if runs_before.is_empty() {
        quote! {
            #[inline]
            fn runs_before() -> ::std::vec::Vec<#arc> {
                ::std::vec::Vec::new()
            }
        }
    } else {
        quote! {
            #[inline]
            fn runs_before() -> ::std::vec::Vec<#arc> {
                ::std::vec![#(#arc::#runs_before),*]
            }
        }
    };
    let runs_after_fn = if runs_after.is_empty() {
        quote! {
            #[inline]
            fn runs_after() -> ::std::vec::Vec<#arc> {
                ::std::vec::Vec::new()
            }
        }
    } else {
        quote! {
            #[inline]
            fn runs_after() -> ::std::vec::Vec<#arc> {
                ::std::vec![#(#arc::#runs_after),*]
            }
        }
    };

    quote! {
        #item

        impl #impl_generics ::hammer_service::data_plane::Feature<#arc>
            for #ident #ty_generics #where_clause
        {
            #[inline]
            fn id() -> #arc {
                #arc::#id
            }

            #runs_before_fn
            #runs_after_fn
        }
    }
    .into()
}

/// Defines a dataplane node-next enum and its compact NodeId table builder.
///
/// Example:
///
/// ```ignore
/// #[hammer_component_macros::node_next]
/// pub enum IpInputNext {
///     Lookup,
///     Reassembly,
/// }
/// ```
#[proc_macro_attribute]
pub fn node_next(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(Span::call_site(), "`node_next` does not accept arguments")
            .to_compile_error()
            .into();
    }
    let item = parse_macro_input!(input as ItemEnum);
    expand_node_next(item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_node(args: NodeArgs, item: ItemStruct) -> Result<TokenStream2> {
    let attrs = item.attrs;
    let vis = item.vis;
    let ident = item.ident;
    let node_name = LitStr::new(
        &to_snake_case(&ident.to_string()).replace('_', "-"),
        ident.span(),
    );
    let generics = item.generics;
    let fields = item.fields;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let mut role_generics = node_role_generics(&generics);
    role_generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(
            #ident #ty_generics: ::hammer_adapter::node::Node
        ));
    let (role_impl_generics, _, role_where_clause) = role_generics.split_for_impl();
    let mut output_fields = Vec::<Field>::new();
    let mut constructor_params = Vec::<TokenStream2>::new();
    let mut constructor_inits = Vec::<TokenStream2>::new();

    match fields {
        Fields::Named(fields) => {
            for field in fields.named {
                let mut field = field;
                let field_args = parse_node_field_attrs(&mut field.attrs)?;
                let ident = field
                    .ident
                    .clone()
                    .ok_or_else(|| Error::new(field.span(), "`node` fields must be named"))?;
                let ty = field.ty.clone();
                if field_args.default.is_some() && field_args.into {
                    return Err(Error::new(
                        ident.span(),
                        "`default` and `into` cannot be combined on a node field",
                    ));
                }
                if let Some(default) = field_args.default {
                    constructor_inits.push(quote!(#ident: #default));
                } else if field_args.into {
                    constructor_params.push(quote!(#ident: impl ::std::convert::Into<#ty>));
                    constructor_inits.push(quote!(#ident: #ident.into()));
                } else {
                    constructor_params.push(quote!(#ident: #ty));
                    constructor_inits.push(quote!(#ident));
                }
                output_fields.push(field);
            }
        }
        Fields::Unit => {}
        Fields::Unnamed(fields) => {
            return Err(Error::new(
                fields.span(),
                "`node` only supports unit structs or structs with named fields",
            ));
        }
    }

    if args.next.is_some() && has_field(&output_fields, "next") {
        return Err(Error::new(
            Span::call_site(),
            "`node(next = ...)` injects a `next` field; remove the field from the struct",
        ));
    }
    if args.next_node && has_field(&output_fields, "next") {
        return Err(Error::new(
            Span::call_site(),
            "`node(next_node)` injects a `next` field; remove the field from the struct",
        ));
    }
    if args.start_arc.is_some() && has_field(&output_fields, "feature_arc") {
        return Err(Error::new(
            Span::call_site(),
            "`node(start_arc = ...)` injects a `feature_arc` field; remove the field from the struct",
        ));
    }
    if (args.next.is_some() || args.sibling_of.is_some()) && has_field(&output_fields, "node_name")
    {
        return Err(Error::new(
            Span::call_site(),
            "`node(next = ...)` and `node(sibling_of = ...)` inject a `node_name` field; remove the field from the struct",
        ));
    }

    let declared_node = args.next.is_some() || args.sibling_of.is_some();
    let mut next_impl = quote!();
    if let Some(next) = &args.next {
        let name_field: Field = parse_quote! {
            node_name: &'static str
        };
        output_fields.push(name_field);
        constructor_inits.push(quote!(node_name: Self::NODE_NAME));
        let field: Field = parse_quote! {
            next: [::hammer_adapter::node::NodeId; #next::COUNT]
        };
        output_fields.push(field);
        constructor_params.push(quote!(next: [::hammer_adapter::node::NodeId; #next::COUNT]));
        constructor_inits.push(quote!(next));
        next_impl = quote! {
            pub const NODE_NEXT_COUNT: usize = #next::COUNT;

            #[inline]
            pub fn runtime_nexts(
                runtime: &::hammer_adapter::DataPlaneRuntime,
            ) -> ::hammer_core::error::CoreResult<[::hammer_adapter::node::NodeId; #next::COUNT]> {
                runtime.current_node_nexts::<{ #next::COUNT }>()
            }
        };
    } else if let Some(sibling_of) = &args.sibling_of {
        let name_field: Field = parse_quote! {
            node_name: &'static str
        };
        output_fields.push(name_field);
        constructor_inits.push(quote!(node_name: Self::NODE_NAME));
        next_impl = quote! {
            pub const NODE_NEXT_COUNT: usize = #sibling_of::NODE_NEXT_COUNT;

            #[inline]
            pub fn runtime_nexts(
                runtime: &::hammer_adapter::DataPlaneRuntime,
            ) -> ::hammer_core::error::CoreResult<
                [::hammer_adapter::node::NodeId; #sibling_of::NODE_NEXT_COUNT]
            > {
                runtime.current_node_nexts::<{ #sibling_of::NODE_NEXT_COUNT }>()
            }
        };
    }

    if args.next_node {
        let field: Field = parse_quote! {
            next: ::hammer_adapter::node::NodeId
        };
        output_fields.push(field);
        constructor_params.push(quote!(next: ::hammer_adapter::node::NodeId));
        constructor_inits.push(quote!(next));
    }

    let start_impl = if let Some(start_arc) = &args.start_arc {
        let field: Field = parse_quote! {
            feature_arc: ::hammer_service::data_plane::FeatureArcStartSlot<#start_arc>
        };
        output_fields.push(field);
        constructor_inits.push(quote!(
            feature_arc: ::hammer_service::data_plane::FeatureArcStartSlot::new()
        ));
        quote! {
            impl #impl_generics ::hammer_service::data_plane::FeatureArcStartNode<#start_arc>
                for #ident #ty_generics #where_clause
            {
                #[inline]
                fn set_feature_arc(
                    &mut self,
                    arc: ::hammer_service::data_plane::FeatureArc<#start_arc>,
                ) {
                    self.feature_arc.set(arc);
                }

                #[inline]
                fn clear_feature_arc(&mut self) {
                    self.feature_arc.clear();
                }
            }
        }
    } else {
        quote!()
    };

    let declared_name_impl = if declared_node {
        quote! {
            #[inline]
            pub fn with_node_name(mut self, node_name: &'static str) -> Self {
                self.node_name = node_name;
                self
            }
        }
    } else {
        quote!()
    };

    let registration_tokens = node_registration_tokens(&ident, &args.next, &args.sibling_of);
    let registration_impl = if args.role.is_some() {
        quote! {
            #[inline]
            pub fn node_registration(&self) -> ::hammer_adapter::node::NodeRegistration {
                #registration_tokens
            }
        }
    } else {
        quote!()
    };
    let initial_nexts_inherent_impl = if args.role.is_some() && args.next.is_some() {
        quote! {
            #[inline]
            pub fn node_initial_nexts(&self) -> &[::hammer_adapter::node::NodeId] {
                &self.next
            }
        }
    } else if args.role.is_some() {
        quote! {
            #[inline]
            pub fn node_initial_nexts(&self) -> &[::hammer_adapter::node::NodeId] {
                &[]
            }
        }
    } else {
        quote!()
    };

    let fields_named: FieldsNamed = parse_quote!({
        #(#output_fields),*
    });

    let role_impl = match args.role {
        Some(NodeRole::Internal) => {
            let initial_nexts = if args.next.is_some() {
                quote! {
                    #[inline]
                    fn node_initial_nexts(&self) -> &[::hammer_adapter::node::NodeId] {
                        self.node_initial_nexts()
                    }
                }
            } else {
                quote!()
            };
            quote! {
                impl #role_impl_generics ::hammer_adapter::node::InternalNode
                    for #ident #ty_generics #role_where_clause
                {
                    #[inline]
                    fn node_registration(&self) -> ::hammer_adapter::node::NodeRegistration {
                        self.node_registration()
                    }

                    #initial_nexts
                }
            }
        }
        Some(NodeRole::Driver) => {
            let initial_nexts = if args.next.is_some() {
                quote! {
                    #[inline]
                    fn node_initial_nexts(&self) -> &[::hammer_adapter::node::NodeId] {
                        self.node_initial_nexts()
                    }
                }
            } else {
                quote!()
            };
            quote! {
                impl #role_impl_generics ::hammer_adapter::node::DriverNode
                    for #ident #ty_generics #role_where_clause
                {
                    #[inline]
                    fn node_registration(&self) -> ::hammer_adapter::node::NodeRegistration {
                        self.node_registration()
                    }

                    #initial_nexts
                }
            }
        }
        None => quote!(),
    };

    Ok(quote! {
        #(#attrs)*
        #vis struct #ident #generics #fields_named

        impl #impl_generics #ident #ty_generics #where_clause {
            pub const NODE_NAME: &'static str = #node_name;

            #[inline]
            pub fn new(#(#constructor_params),*) -> Self {
                Self {
                    #(#constructor_inits),*
                }
            }

            #declared_name_impl
            #registration_impl
            #initial_nexts_inherent_impl
            #next_impl
        }

        #start_impl
        #role_impl
    })
}

fn node_registration_tokens(
    ident: &Ident,
    next: &Option<Path>,
    sibling_of: &Option<Path>,
) -> TokenStream2 {
    if let Some(next) = next {
        quote!(::hammer_adapter::node::NodeRegistration::next(self.node_name, #next::COUNT))
    } else if let Some(sibling_of) = sibling_of {
        quote!(::hammer_adapter::node::NodeRegistration::sibling_of(
            self.node_name,
            #sibling_of::NODE_NAME
        ))
    } else {
        let _ = ident;
        quote!(::hammer_adapter::node::NodeRegistration::Plain)
    }
}

fn node_role_generics(generics: &Generics) -> Generics {
    let mut generics = generics.clone();
    for param in generics.params.iter_mut() {
        match param {
            GenericParam::Type(param) => param.default = None,
            GenericParam::Const(param) => param.default = None,
            GenericParam::Lifetime(_) => {}
        }
    }
    generics
}

fn has_field(fields: &[Field], name: &str) -> bool {
    fields
        .iter()
        .any(|field| field.ident.as_ref().is_some_and(|ident| ident == name))
}

fn parse_node_field_attrs(attrs: &mut Vec<Attribute>) -> Result<NodeFieldArgs> {
    let mut node_args = NodeFieldArgs::default();
    let mut retained = Vec::with_capacity(attrs.len());
    for attr in attrs.drain(..) {
        if !attr.path().is_ident("node") {
            retained.push(attr);
            continue;
        }
        let args = attr.parse_args::<NodeFieldArgs>()?;
        if node_args.default.is_some() && args.default.is_some() {
            return Err(Error::new(
                attr.span(),
                "duplicate `default` field argument",
            ));
        }
        if node_args.into && args.into {
            return Err(Error::new(attr.span(), "duplicate `into` field argument"));
        }
        if args.default.is_some() {
            node_args.default = args.default;
        }
        node_args.into |= args.into;
    }
    *attrs = retained;
    Ok(node_args)
}

fn expand_node_next(item: ItemEnum) -> Result<TokenStream2> {
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(Error::new(
            item.generics.span(),
            "`node_next` does not support generic next enums",
        ));
    }
    if item.variants.is_empty() {
        return Err(Error::new(
            item.ident.span(),
            "`node_next` requires at least one variant",
        ));
    }

    let attrs = item.attrs;
    let vis = item.vis;
    let ident = item.ident;
    let mut variant_defs = Vec::with_capacity(item.variants.len());
    let mut variant_idents = Vec::with_capacity(item.variants.len());
    let mut node_params = Vec::with_capacity(item.variants.len());

    for variant in item.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new(
                variant.fields.span(),
                "`node_next` variants must be unit variants",
            ));
        }
        if let Some((eq_token, _)) = variant.discriminant {
            return Err(Error::new(
                eq_token.span(),
                "`node_next` assigns variant slots automatically",
            ));
        }

        let variant_attrs = variant.attrs;
        let variant_ident = variant.ident;
        let node_param = format_ident!("{}_node", to_snake_case(&variant_ident.to_string()));
        variant_defs.push(quote! {
            #(#variant_attrs)*
            #variant_ident
        });
        variant_idents.push(variant_ident);
        node_params.push(node_param);
    }

    let count = variant_idents.len();
    Ok(quote! {
        #(#attrs)*
        #[repr(usize)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #vis enum #ident {
            #(#variant_defs),*
        }

        impl #ident {
            pub const COUNT: usize = #count;
            pub const VARIANTS: [Self; Self::COUNT] = [
                #(Self::#variant_idents),*
            ];

            #[inline(always)]
            pub const fn slot(self) -> usize {
                self as usize
            }

            #[inline(always)]
            pub const fn nodes(
                #(#node_params: ::hammer_adapter::node::NodeId),*
            ) -> [::hammer_adapter::node::NodeId; Self::COUNT] {
                [#(#node_params),*]
            }
        }

        impl ::hammer_adapter::node::NodeNext for #ident {
            const COUNT: usize = #ident::COUNT;

            #[inline(always)]
            fn slot(self) -> usize {
                self as usize
            }
        }

        const _: () = {
            assert!(#ident::COUNT <= ::hammer_adapter::node::MAX_NODE_NEXT_FRAMES);
        };
    })
}

fn to_snake_case(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut previous_was_lower_or_digit = false;
    for ch in input.chars() {
        if ch.is_ascii_uppercase() {
            if previous_was_lower_or_digit {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
            previous_was_lower_or_digit = false;
        } else {
            previous_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
            output.push(ch);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_args_rejects_next_with_sibling_of() {
        let err = match syn::parse_str::<NodeArgs>(
            "role = internal, next = OwnerNext, sibling_of = OwnerNode",
        ) {
            Ok(_) => panic!("next and sibling_of should be mutually exclusive"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("`next` and `sibling_of` are mutually exclusive"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn node_args_accepts_sibling_of_type() {
        let args = syn::parse_str::<NodeArgs>("role = internal, sibling_of = OwnerNode")
            .expect("parse sibling_of");

        assert!(args.next.is_none());
        assert!(matches!(args.role, Some(NodeRole::Internal)));
        assert_eq!(
            args.sibling_of
                .as_ref()
                .and_then(|path| path.segments.last())
                .map(|segment| segment.ident.to_string())
                .as_deref(),
            Some("OwnerNode")
        );
    }

    #[test]
    fn declared_node_expansion_supports_instance_node_name_override() {
        let args =
            syn::parse_str::<NodeArgs>("role = internal, next = OwnerNext").expect("parse next");
        let item = syn::parse_str::<ItemStruct>("pub struct OwnerNode;").expect("parse item");
        let expanded = expand_node(args, item).expect("expand node").to_string();

        assert!(
            expanded.contains("with_node_name"),
            "declared node should expose node name override: {expanded}"
        );
        assert!(
            expanded.contains("node_name"),
            "declared node should store instance node name: {expanded}"
        );
    }

    #[test]
    fn plain_node_expansion_does_not_inject_node_name_override() {
        let args = syn::parse_str::<NodeArgs>("").expect("parse plain");
        let item = syn::parse_str::<ItemStruct>("pub struct PlainNode;").expect("parse item");
        let expanded = expand_node(args, item).expect("expand node").to_string();

        assert!(
            !expanded.contains("with_node_name"),
            "plain node should not expose node name override: {expanded}"
        );
        assert!(
            !expanded.contains("node_name"),
            "plain node should not store instance node name: {expanded}"
        );
    }
}
