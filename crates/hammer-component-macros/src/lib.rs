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
    Event,
}

impl ComponentKind {
    fn parse(ident: &Ident) -> Result<Self> {
        match ident.to_string().as_str() {
            "event" => Ok(Self::Event),
            other => Err(Error::new(
                ident.span(),
                format!("unknown component kind `{other}`; expected event"),
            )),
        }
    }

    fn trait_path(self) -> TokenStream2 {
        quote!(crate::component_registry::EventSubscriberComponentDeclaration)
    }

    fn kind_name(self) -> &'static str {
        "event"
    }
}

struct ComponentArgs {
    kind: ComponentKind,
    name: LitStr,
    builder: ExprPath,
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
                            "unknown argument `{other}`; expected `name`, `builder`, `metrics`, or `runtime`"
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
/// #[hammer_component_macros::hammer_component(event, name = "metrics", builder = build_metrics_subscriber)]
/// struct MetricsEventSubscriber;
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

    let id_value = quote!(#meta_name.to_owned());
    let networks_value = quote!(Vec::new());
    let dependencies_value = quote!(Vec::new());
    let metrics_value = if let Some((module, component_type)) = args.metrics {
        quote!(Some(::hammer_runtime::ComponentMetricsMeta {
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

    let declaration_impl = quote! {
        #declaration_impl_head {
            const TYPE_NAME: &'static str = #name;

            fn build(
                logger: ::hammer_core::log::Logger,
                control_handle: ::std::sync::Arc<crate::ControlThreadHandle>,
            ) -> ::hammer_core::error::HammerResult<::std::vec::Vec<crate::ControlEventSubscriptionHandle>> {
                #builder(logger, control_handle)
            }
        }
    };

    quote! {
        #item

        impl #impl_generics ::hammer_runtime::ComponentMetadata for #ident #ty_generics #where_clause {
            fn component_meta(&self) -> ::hammer_runtime::ComponentMeta {
                ::hammer_runtime::ComponentMeta::new(
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
    let node_name = LitStr::new(&graph_node_name_from_ident(&ident), ident.span());
    let generics = item.generics;
    let fields = item.fields;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let mut role_generics = node_role_generics(&generics);
    role_generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(
            #ident #ty_generics: ::hammer_runtime::node::Node
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
            next: [::hammer_core::data_plane::NodeId; #next::COUNT]
        };
        output_fields.push(field);
        constructor_params.push(quote!(next: [::hammer_core::data_plane::NodeId; #next::COUNT]));
        constructor_inits.push(quote!(next));
        next_impl = quote! {
            pub const NODE_NEXT_COUNT: usize = #next::COUNT;

            #[inline]
            pub fn runtime_nexts(
                runtime: &::hammer_runtime::DataPlaneRuntime,
            ) -> ::hammer_core::error::CoreResult<[::hammer_core::data_plane::NodeId; #next::COUNT]> {
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
                runtime: &::hammer_runtime::DataPlaneRuntime,
            ) -> ::hammer_core::error::CoreResult<
                [::hammer_core::data_plane::NodeId; #sibling_of::NODE_NEXT_COUNT]
            > {
                runtime.current_node_nexts::<{ #sibling_of::NODE_NEXT_COUNT }>()
            }
        };
    }

    if args.next_node {
        let field: Field = parse_quote! {
            next: ::hammer_core::data_plane::NodeId
        };
        output_fields.push(field);
        constructor_params.push(quote!(next: ::hammer_core::data_plane::NodeId));
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
            pub fn node_registration(&self) -> ::hammer_core::data_plane::NodeRegistration {
                #registration_tokens
            }
        }
    } else {
        quote!()
    };
    let initial_nexts_inherent_impl = if args.role.is_some() && args.next.is_some() {
        quote! {
            #[inline]
            pub fn node_initial_nexts(&self) -> &[::hammer_core::data_plane::NodeId] {
                &self.next
            }
        }
    } else if args.role.is_some() {
        quote! {
            #[inline]
            pub fn node_initial_nexts(&self) -> &[::hammer_core::data_plane::NodeId] {
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
                    fn node_initial_nexts(&self) -> &[::hammer_core::data_plane::NodeId] {
                        self.node_initial_nexts()
                    }
                }
            } else {
                quote!()
            };
            quote! {
                impl #role_impl_generics ::hammer_runtime::node::InternalNode
                    for #ident #ty_generics #role_where_clause
                {
                    #[inline]
                    fn node_registration(&self) -> ::hammer_core::data_plane::NodeRegistration {
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
                    fn node_initial_nexts(&self) -> &[::hammer_core::data_plane::NodeId] {
                        self.node_initial_nexts()
                    }
                }
            } else {
                quote!()
            };
            quote! {
                impl #role_impl_generics ::hammer_runtime::node::DriverNode
                    for #ident #ty_generics #role_where_clause
                {
                    #[inline]
                    fn node_registration(&self) -> ::hammer_core::data_plane::NodeRegistration {
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
        quote!(::hammer_core::data_plane::NodeRegistration::next(self.node_name, #next::COUNT))
    } else if let Some(sibling_of) = sibling_of {
        quote!(::hammer_core::data_plane::NodeRegistration::sibling_of(
            self.node_name,
            #sibling_of::NODE_NAME
        ))
    } else {
        let _ = ident;
        quote!(::hammer_core::data_plane::NodeRegistration::Plain)
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

fn parse_variant_next_name(attrs: &mut Vec<Attribute>) -> Result<Option<String>> {
    let mut retained = Vec::with_capacity(attrs.len());
    let mut next_name = None;
    for attr in attrs.drain(..) {
        if !attr.path().is_ident("next") {
            retained.push(attr);
            continue;
        }
        if next_name.is_some() {
            return Err(Error::new(
                attr.span(),
                "duplicate `next` variant attribute",
            ));
        }
        let lit = attr
            .parse_args::<LitStr>()
            .map_err(|_| Error::new(attr.span(), "#[next(...)] expects a single string literal"))?;
        next_name = Some(lit.value());
    }
    *attrs = retained;
    Ok(next_name)
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

    let mut next_names = Vec::with_capacity(item.variants.len());

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

        let mut variant_attrs = variant.attrs;
        let variant_ident = variant.ident;
        let node_param = format_ident!("{}_node", to_snake_case(&variant_ident.to_string()));
        let next_name = match parse_variant_next_name(&mut variant_attrs)? {
            Some(name) => name,
            None => to_snake_case(&variant_ident.to_string()),
        };
        next_names.push(LitStr::new(&next_name, variant_ident.span()));
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
            pub const NEXT_NAMES: [&'static str; Self::COUNT] = [#(#next_names),*];
            pub const VARIANTS: [Self; Self::COUNT] = [
                #(Self::#variant_idents),*
            ];

            #[inline(always)]
            pub const fn slot(self) -> usize {
                self as usize
            }

            #[inline(always)]
            pub const fn nodes(
                #(#node_params: ::hammer_core::data_plane::NodeId),*
            ) -> [::hammer_core::data_plane::NodeId; Self::COUNT] {
                [#(#node_params),*]
            }
        }

        impl ::hammer_core::data_plane::NodeNext for #ident {
            const COUNT: usize = #ident::COUNT;

            #[inline(always)]
            fn slot(self) -> usize {
                self as usize
            }
        }

        const _: () = {
            assert!(#ident::COUNT <= ::hammer_core::data_plane::MAX_NODE_NEXT_SLOTS);
        };
    })
}

fn graph_node_name_from_ident(ident: &Ident) -> String {
    let snake = to_snake_case(&ident.to_string()).replace('_', "-");
    snake
        .strip_suffix("-node")
        .map(|s| s.to_string())
        .unwrap_or(snake)
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

struct GraphNodeArgs {
    graph: Ident,
    /// User-supplied init function: `fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId>`.
    /// The macro does not generate the init body; it only references this fn in the
    /// `NodeEntry`. The fn reads its own subsystem's `*_MAIN` global (or registers
    /// directly) — the macro never knows protocol details.
    init: Option<Path>,
    kind: Option<Ident>,
    name: Option<LitStr>,
    next: Option<Path>,
    role: Option<NodeRole>,
    next_node: bool,
    sibling_of: Option<Path>,
    start_arc: Option<Path>,
}

impl Default for GraphNodeArgs {
    fn default() -> Self {
        Self {
            graph: Ident::new("_", Span::call_site()),
            init: None,
            kind: None,
            name: None,
            next: None,
            role: None,
            next_node: false,
            sibling_of: None,
            start_arc: None,
        }
    }
}

impl Parse for GraphNodeArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = GraphNodeArgs::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "next_node" => {
                    if args.next_node {
                        return Err(Error::new(key.span(), "duplicate `next_node` argument"));
                    }
                    args.next_node = true;
                }
                _ => {
                    input.parse::<Token![=]>()?;
                    match key.to_string().as_str() {
                        "graph" => args.graph = input.parse()?,
                        "init" => args.init = Some(input.parse()?),
                        "kind" => {
                            let kind: Ident = input.parse()?;
                            args.kind = Some(match kind.to_string().as_str() {
                                "internal" | "driver" | "handoff" => kind,
                                other => {
                                    return Err(Error::new(
                                        kind.span(),
                                        format!(
                                            "unknown graph node kind `{other}`; expected `internal`, `driver`, or `handoff`"
                                        ),
                                    ));
                                }
                            });
                        }
                        "name" => args.name = Some(input.parse()?),
                        "next" => args.next = Some(input.parse()?),
                        "role" => {
                            if args.role.is_some() {
                                return Err(Error::new(key.span(), "duplicate `role` argument"));
                            }
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
                        "sibling_of" => {
                            if args.sibling_of.is_some() {
                                return Err(Error::new(
                                    key.span(),
                                    "duplicate `sibling_of` argument",
                                ));
                            }
                            args.sibling_of = Some(input.parse()?);
                        }
                        "start_arc" => {
                            if args.start_arc.is_some() {
                                return Err(Error::new(
                                    key.span(),
                                    "duplicate `start_arc` argument",
                                ));
                            }
                            args.start_arc = Some(input.parse()?);
                        }
                        other => {
                            return Err(Error::new(
                                key.span(),
                                format!(
                                    "unknown `graph_node` argument `{other}`; expected `graph`, `init`, `kind`, `name`, `next`, `role`, `next_node`, `sibling_of`, or `start_arc`"
                                ),
                            ));
                        }
                    }
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        if args.graph == Ident::new("_", Span::call_site()) {
            return Err(Error::new(Span::call_site(), "missing `graph` argument"));
        }
        if args.init.is_none() {
            return Err(Error::new(
                Span::call_site(),
                "missing `init` argument (path to a `fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId>`)",
            ));
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

fn graph_slice_path(graph: &Ident) -> Path {
    let slice = format_ident!("{}_GRAPH_NODES", graph.to_string().to_ascii_uppercase());
    parse_quote!(crate::packet_graph::#slice)
}

fn graph_node_registration(name: &TokenStream2, next: Option<&Path>) -> TokenStream2 {
    match next {
        Some(next) => {
            quote!(::hammer_core::data_plane::NodeRegistration::next(#name, #next::COUNT))
        }
        None => quote!(::hammer_core::data_plane::NodeRegistration::next(#name, 0)),
    }
}

fn graph_node_kind_expr(kind: Option<&Ident>) -> TokenStream2 {
    match kind.map(|k| k.to_string()).as_deref() {
        Some("driver") => quote!(::hammer_core::data_plane::NodeKind::Driver),
        // `internal` and `handoff` are both `NodeKind::Internal`; handoff nodes
        // register with a handle inside their own init fn, the kind stays Internal.
        Some("internal") | Some("handoff") | None => {
            quote!(::hammer_core::data_plane::NodeKind::Internal)
        }
        Some(other) => quote! {
            compile_error!(concat!("unknown graph node kind: ", #other))
        },
    }
}

/// Registers a struct as a graph node via linkme `NodeEntry`.
///
/// Emits a `NodeEntry` collected into the `{graph}_GRAPH_NODES` linkme slice.
/// The node's init function is supplied by `init = path` and must have the
/// signature `fn(&DataPlaneRuntime, usize) -> CoreResult<NodeId>`; the macro
/// does not generate the init body. The init fn reads its own subsystem's
/// `*_MAIN` global (or registers the node directly) — the macro never knows
/// protocol details, so new transports (e.g. quic) plug in without touching
/// the macro. `DataPlaneRuntime::init_graph` walks the slice and calls each
/// `init`; `resolve_named_next_nodes` then resolves next-node names (VPP
/// `vlib_node_main_init`).
/// ```ignore
/// #[hammer_component_macros::graph_node(graph = service, init = my::register_foo)]
/// pub struct FooNode;
/// ```
#[proc_macro_attribute]
pub fn graph_node(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as GraphNodeArgs);
    let item = parse_macro_input!(input as Item);
    let ident = match item {
        Item::Struct(ref item) => item.ident.clone(),
        _ => {
            return Error::new(item.span(), "`graph_node` can only be attached to a struct")
                .to_compile_error()
                .into();
        }
    };
    expand_graph_node(args, &ident, item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_graph_node(args: GraphNodeArgs, ident: &Ident, item: Item) -> Result<TokenStream2> {
    let graph_slice = graph_slice_path(&args.graph);
    let static_ident = format_ident!(
        "__{}_GRAPH_NODE_{}",
        args.graph.to_string().to_ascii_uppercase(),
        ident
    );
    let node_kind = graph_node_kind_expr(args.kind.as_ref());
    let node_name = if let Some(name) = &args.name {
        quote!(#name)
    } else {
        quote!(#ident::NODE_NAME)
    };
    let node_registration = graph_node_registration(&node_name, args.next.as_ref());
    let init = args
        .init
        .as_ref()
        .expect("graph_node `init` argument validated in parse");

    let registration = quote! {
        #[::linkme::distributed_slice(#graph_slice)]
        static #static_ident: ::hammer_runtime::NodeEntry = ::hammer_runtime::NodeEntry {
            registration: #node_registration,
            kind: #node_kind,
            init: #init,
        };
    };

    // Node expansion is triggered when node-specific args (role, next_node, etc.)
    // are present in #[graph_node(...)], or when struct fields carry #[node(default)]
    // field-level attrs (enabling callers to drop standalone #[node]).
    let has_field_node_attr = match &item {
        Item::Struct(s) => s
            .fields
            .iter()
            .any(|f| f.attrs.iter().any(|a| a.path().is_ident("node"))),
        _ => false,
    };
    let needs_node_expansion = args.role.is_some()
        || args.next_node
        || args.sibling_of.is_some()
        || args.start_arc.is_some()
        || has_field_node_attr;

    if needs_node_expansion {
        let struct_item = match item {
            Item::Struct(s) => s,
            _ => {
                return Err(Error::new(
                    Span::call_site(),
                    "graph_node with node args requires a struct",
                ));
            }
        };
        let node_args = NodeArgs {
            next: args.next.clone(),
            next_node: args.next_node,
            sibling_of: args.sibling_of.clone(),
            role: args.role,
            start_arc: args.start_arc.clone(),
        };
        let node_output = expand_node(node_args, struct_item)?;
        Ok(quote! {
            #node_output
            #registration
        })
    } else {
        Ok(quote! {
            #item
            #registration
        })
    }
}

// ── Init / Config function macros (VPP VLIB_INIT_FUNCTION / VLIB_CONFIG_FUNCTION) ──

struct InitFnArgs {
    name: LitStr,
    runs_before: Vec<LitStr>,
    runs_after: Vec<LitStr>,
}

impl Parse for InitFnArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut runs_before = Vec::new();
        let mut runs_after = Vec::new();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(Error::new(key.span(), "duplicate `name` argument"));
                    }
                    name = Some(input.parse()?);
                }
                "runs_before" => runs_before = parse_litstr_array(input)?,
                "runs_after" => runs_after = parse_litstr_array(input)?,
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown argument `{other}`; expected `name`, `runs_before`, or `runs_after`"
                        ),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        Ok(Self {
            name: name.ok_or_else(|| Error::new(Span::call_site(), "missing `name` argument"))?,
            runs_before,
            runs_after,
        })
    }
}

struct ConfigFnArgs {
    name: LitStr,
    early: bool,
}

impl Parse for ConfigFnArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut early = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(Error::new(key.span(), "duplicate `name` argument"));
                    }
                    name = Some(input.parse()?);
                }
                "early" => {
                    if early.is_some() {
                        return Err(Error::new(key.span(), "duplicate `early` argument"));
                    }
                    let v: Ident = input.parse()?;
                    early = Some(match v.to_string().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(Error::new(v.span(), "expected `true` or `false`")),
                    });
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!("unknown argument `{other}`; expected `name` or `early`"),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        Ok(Self {
            name: name.ok_or_else(|| Error::new(Span::call_site(), "missing `name` argument"))?,
            early: early.unwrap_or(false),
        })
    }
}

struct WorkerInitFnArgs {
    name: LitStr,
    runs_after: Vec<LitStr>,
}

impl Parse for WorkerInitFnArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut runs_after = Vec::new();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(Error::new(key.span(), "duplicate `name` argument"));
                    }
                    name = Some(input.parse()?);
                }
                "runs_after" => runs_after = parse_litstr_array(input)?,
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!("unknown argument `{other}`; expected `name` or `runs_after`"),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        Ok(Self {
            name: name.ok_or_else(|| Error::new(Span::call_site(), "missing `name` argument"))?,
            runs_after,
        })
    }
}

fn parse_litstr_array(input: ParseStream<'_>) -> Result<Vec<LitStr>> {
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

fn init_function_static_name(fn_name: &LitStr) -> Ident {
    let name_str = fn_name.value();
    let sanitized: String = name_str
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format_ident!("__INIT_FN_{}", sanitized.to_ascii_uppercase())
}

/// Registers a function as an init function in the topologically-sorted init chain.
///
/// Example:
/// ```ignore
/// #[init_function(name = "tcp_init", runs_after = ["buffer_main_init"], runs_before = ["session_init"])]
/// fn tcp_init(vm: &mut EngineMain) -> Result<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn init_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as InitFnArgs);
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    let fn_name = &fn_item.sig.ident;
    let name = args.name;
    let runs_before = args.runs_before;
    let runs_after = args.runs_after;
    let static_ident = init_function_static_name(&name);

    let expanded = quote! {
        #fn_item

        #[::linkme::distributed_slice(::hammer_runtime::init::INIT_FUNCTIONS)]
        static #static_ident: ::hammer_runtime::init::InitFunction = ::hammer_runtime::init::InitFunction {
            name: #name,
            runs_before: &[#(#runs_before),*],
            runs_after: &[#(#runs_after),*],
            func: #fn_name,
        };
    };
    expanded.into()
}

/// Registers a function as a config function for TOML block dispatching.
///
/// Example:
/// ```ignore
/// #[config_function(name = "tcp", early = false)]
/// fn tcp_config(vm: &mut EngineMain, input: &toml::Value) -> Result<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn config_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ConfigFnArgs);
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    let fn_name = &fn_item.sig.ident;
    let name = args.name;
    let early = args.early;
    let static_ident = init_function_static_name(&name);
    let slice = if early {
        quote!(::hammer_runtime::init::EARLY_CONFIG_FUNCTIONS)
    } else {
        quote!(::hammer_runtime::init::CONFIG_FUNCTIONS)
    };

    let expanded = quote! {
        #fn_item

        #[::linkme::distributed_slice(#slice)]
        static #static_ident: ::hammer_runtime::init::ConfigFunction = ::hammer_runtime::init::ConfigFunction {
            name: #name,
            func: #fn_name,
        };
    };
    expanded.into()
}

/// Shorthand for `#[config_function(name = "...", early = true)]`.
#[proc_macro_attribute]
pub fn early_config_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args: proc_macro2::TokenStream = args.into();
    let attr = quote!(name = #args, early = true);
    config_function(attr.into(), input)
}

/// Registers a function to run at main-loop-enter time (e.g., `start_workers`).
///
/// Example:
/// ```ignore
/// #[main_loop_enter_function]
/// fn start_workers(vm: &mut EngineMain) -> Result<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn main_loop_enter_function(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(
            Span::call_site(),
            "`main_loop_enter_function` does not accept arguments",
        )
        .to_compile_error()
        .into();
    }
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    let fn_name = &fn_item.sig.ident;
    let static_ident = format_ident!("__MAIN_LOOP_ENTER_{}", fn_name);

    let expanded = quote! {
        #fn_item

        #[::linkme::distributed_slice(::hammer_runtime::init::MAIN_LOOP_ENTER_FUNCTIONS)]
        static #static_ident: ::hammer_runtime::init::InitFunction = ::hammer_runtime::init::InitFunction {
            name: stringify!(#fn_name),
            runs_before: &[],
            runs_after: &[],
            func: #fn_name,
        };
    };
    expanded.into()
}

/// Registers a function to run at main-loop-exit time.
#[proc_macro_attribute]
pub fn main_loop_exit_function(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(
            Span::call_site(),
            "`main_loop_exit_function` does not accept arguments",
        )
        .to_compile_error()
        .into();
    }
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    let fn_name = &fn_item.sig.ident;
    let static_ident = format_ident!("__MAIN_LOOP_EXIT_{}", fn_name);

    let expanded = quote! {
        #fn_item

        #[::linkme::distributed_slice(::hammer_runtime::init::MAIN_LOOP_EXIT_FUNCTIONS)]
        static #static_ident: ::hammer_runtime::init::InitFunction = ::hammer_runtime::init::InitFunction {
            name: stringify!(#fn_name),
            runs_before: &[],
            runs_after: &[],
            func: #fn_name,
        };
    };
    expanded.into()
}

/// Registers a per-worker init function in the topologically-sorted worker init chain.
///
/// Example:
/// ```ignore
/// #[worker_init_function(name = "tcp_worker_init", runs_after = ["generic_worker_init"])]
/// fn tcp_worker_init(vm: &mut EngineMain) -> Result<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn worker_init_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as WorkerInitFnArgs);
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    let fn_name = &fn_item.sig.ident;
    let name = args.name;
    let runs_after = args.runs_after;
    let static_ident = init_function_static_name(&name);

    let expanded = quote! {
        #fn_item

        #[::linkme::distributed_slice(::hammer_runtime::init::WORKER_INIT_FUNCTIONS)]
        static #static_ident: ::hammer_runtime::init::InitFunction = ::hammer_runtime::init::InitFunction {
            name: #name,
            runs_before: &[],
            runs_after: &[#(#runs_after),*],
            func: #fn_name,
        };
    };
    expanded.into()
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

    #[test]
    fn node_next_reads_next_attr_for_names() {
        let item = syn::parse_str::<ItemEnum>(
            r#"
            pub enum SampleNext {
                #[next("custom-node")]
                Custom,
                Fallback,
            }
            "#,
        )
        .expect("parse enum");
        let expanded = expand_node_next(item)
            .expect("expand node_next")
            .to_string();

        assert!(
            expanded.contains(r#""custom-node""#),
            "expected explicit next name: {expanded}"
        );
        assert!(
            expanded.contains(r#""fallback""#),
            "expected snake-case fallback name: {expanded}"
        );
        assert!(
            !expanded.contains("# [next"),
            "next attr must be stripped from emitted variants: {expanded}"
        );
    }
}

struct IpcHandlerAttrArgs {
    name: String,
}

impl Parse for IpcHandlerAttrArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name_ident: Ident = input.parse()?;
        if name_ident != "name" {
            return Err(Error::new(name_ident.span(), "expected `name`"));
        }
        let _eq_token: Token![=] = input.parse()?;
        let name: LitStr = input.parse()?;
        Ok(Self { name: name.value() })
    }
}

/// Attribute macro for registering an IPC handler function.
///
/// # Usage
///
/// ```ignore
/// #[hammer_component_macros::ipc_handler(name = "ping")]
/// fn handle_ping(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn ipc_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as IpcHandlerAttrArgs);
    let input = parse_macro_input!(item as syn::ItemFn);
    let name = &args.name;
    let fn_name = &input.sig.ident;
    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;
    let static_name = format_ident!("__IPC_HANDLER_{}", fn_name);

    let expanded = quote! {
        #vis #sig
        #block

        #[::linkme::distributed_slice(::hammer_ipc::IPC_HANDLERS)]
        static #static_name: ::hammer_ipc::IpcHandler = ::hammer_ipc::IpcHandler {
            name: #name,
            handler: #fn_name,
        };
    };
    TokenStream::from(expanded)
}
