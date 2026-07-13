use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Error, Expr, ExprPath, Field, Fields, FieldsNamed, GenericParam, Generics, Ident,
    Item, ItemEnum, ItemFn, ItemStruct, LitStr, Path, Result, Token, Type, bracketed,
    parenthesized, parse_macro_input, parse_quote, spanned::Spanned,
};

struct NodeFunctionArgs {
    node: Path,
}

impl Parse for NodeFunctionArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut node = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "node" => node = Some(input.parse()?),
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!("unknown `node_function` argument `{other}`; expected `node`"),
                    ));
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            node: node.ok_or_else(|| Error::new(Span::call_site(), "missing `node` argument"))?,
        })
    }
}

/// Registers multiarch Node Function candidates for an existing Graph Node.
///
/// The function is compiled once per instruction-set variant supported by the
/// target architecture. Conditional compilation attributes on the function gate
/// every generated variant.
#[proc_macro_attribute]
pub fn node_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as NodeFunctionArgs);
    let function = parse_macro_input!(input as ItemFn);
    expand_node_function(args, function).into()
}

fn expand_node_function(args: NodeFunctionArgs, function: ItemFn) -> TokenStream2 {
    let node = &args.node;
    let function_name = function.sig.ident.clone();
    // VPP recompiles one VLIB_NODE_FN body for each enabled march variant.
    // Generate the equivalent private symbols from one Rust declaration.
    let scalar = expand_node_function_variant(
        node,
        &function,
        function_name,
        "scalar",
        quote!(::hammer_runtime::DataPlaneInstructionSet::Scalar),
        quote!(),
        quote!(),
    );
    let x86_architecture = quote!(#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]);
    let sse2 = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_sse2", function.sig.ident),
        "sse2",
        quote!(::hammer_runtime::DataPlaneInstructionSet::Sse2),
        x86_architecture.clone(),
        quote!(#[target_feature(enable = "sse2")]),
    );
    let avx2 = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_avx2", function.sig.ident),
        "avx2",
        quote!(::hammer_runtime::DataPlaneInstructionSet::Avx2),
        x86_architecture.clone(),
        quote!(#[target_feature(enable = "avx2")]),
    );
    let avx512 = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_avx512", function.sig.ident),
        "avx512",
        quote!(::hammer_runtime::DataPlaneInstructionSet::Avx512),
        x86_architecture,
        quote!(#[target_feature(enable = "avx512f")]),
    );
    let neon = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_neon", function.sig.ident),
        "neon",
        quote!(::hammer_runtime::DataPlaneInstructionSet::Neon),
        quote!(#[cfg(any(target_arch = "arm", target_arch = "aarch64"))]),
        quote!(#[target_feature(enable = "neon")]),
    );

    quote! {
        #scalar
        #sse2
        #avx2
        #avx512
        #neon
    }
}

fn expand_node_function_variant(
    node: &Path,
    function: &ItemFn,
    function_name: Ident,
    suffix: &str,
    instruction_set: TokenStream2,
    architecture: TokenStream2,
    target_feature: TokenStream2,
) -> TokenStream2 {
    let mut variant_function = function.clone();
    variant_function.sig.ident = function_name.clone();
    if !target_feature.is_empty() && variant_function.sig.unsafety.is_none() {
        variant_function.sig.unsafety = Some(parse_quote!(unsafe));
    }
    let input_cfg = function.attrs.iter().filter(|attribute| {
        attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
    });
    let static_name = format_ident!(
        "__NODE_FUNCTION_{}_{}",
        function.sig.ident.to_string().to_ascii_uppercase(),
        suffix.to_ascii_uppercase(),
    );
    quote! {
        #architecture
        #target_feature
        #variant_function

        #architecture
        #(#input_cfg)*
        #[::linkme::distributed_slice(::hammer_runtime::node::NODE_FUNCTIONS)]
        static #static_name: ::hammer_runtime::node::NodeFunctionRegistration = unsafe {
            ::hammer_runtime::node::NodeFunctionRegistration::new(
                #node::NODE_NAME,
                #instruction_set,
                #function_name,
            )
        };
    }
}

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeRole {
    Internal,
    Driver,
}

#[derive(Clone, Copy)]
enum GraphNodeState {
    Polling,
    Interrupt,
    Disabled,
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
    expand_node(args, item, None, true)
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

fn expand_node(
    args: NodeArgs,
    item: ItemStruct,
    declared_name: Option<LitStr>,
    allow_name_override: bool,
) -> Result<TokenStream2> {
    let attrs = item.attrs;
    let vis = item.vis;
    let ident = item.ident;
    let node_name = declared_name
        .unwrap_or_else(|| LitStr::new(&graph_node_name_from_ident(&ident), ident.span()));
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
        if allow_name_override {
            let name_field: Field = parse_quote! {
                node_name: &'static str
            };
            output_fields.push(name_field);
            constructor_inits.push(quote!(node_name: Self::NODE_NAME));
        }
        let field: Field = parse_quote! {
            next: [::hammer_core::data_plane::NodeId; #next::COUNT]
        };
        output_fields.push(field);
        constructor_params.push(quote!(next: [::hammer_core::data_plane::NodeId; #next::COUNT]));
        constructor_inits.push(quote!(next));
        next_impl = quote! {
            pub const NODE_NEXT_COUNT: usize = #next::COUNT;
        };
    } else if let Some(sibling_of) = &args.sibling_of {
        if allow_name_override {
            let name_field: Field = parse_quote! {
                node_name: &'static str
            };
            output_fields.push(name_field);
            constructor_inits.push(quote!(node_name: Self::NODE_NAME));
        }
        next_impl = quote! {
            pub const NODE_NEXT_COUNT: usize = #sibling_of::NODE_NEXT_COUNT;
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

    let declared_name_impl = if declared_node && allow_name_override {
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

    let registration_tokens =
        node_registration_tokens(&ident, &args.next, &args.sibling_of, allow_name_override);
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
    allow_name_override: bool,
) -> TokenStream2 {
    let name = if allow_name_override {
        quote!(self.node_name)
    } else {
        quote!(Self::NODE_NAME)
    };
    if let Some(next) = next {
        quote!(::hammer_core::data_plane::NodeRegistration::next(#name, #next::COUNT))
    } else if let Some(sibling_of) = sibling_of {
        quote!(::hammer_core::data_plane::NodeRegistration::sibling_of(
            #name,
            #sibling_of::NODE_NAME
        ))
    } else {
        let _ = ident;
        if allow_name_override {
            quote!(::hammer_core::data_plane::NodeRegistration::Plain)
        } else {
            quote!(::hammer_core::data_plane::NodeRegistration::next(
                Self::NODE_NAME,
                0,
            ))
        }
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
            #[inline(always)]
            fn slot(self) -> u16 {
                self as u16
            }
        }

        const _: () = {
            assert!(#ident::COUNT <= u16::MAX as usize + 1);
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
    init: Option<Path>,
    kind: Option<Ident>,
    name: Option<LitStr>,
    next: Option<Path>,
    role: Option<NodeRole>,
    state: Option<GraphNodeState>,
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
            state: None,
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
                        "state" => {
                            if args.state.is_some() {
                                return Err(Error::new(key.span(), "duplicate `state` argument"));
                            }
                            let state: Ident = input.parse()?;
                            args.state = Some(match state.to_string().as_str() {
                                "polling" => GraphNodeState::Polling,
                                "interrupt" => GraphNodeState::Interrupt,
                                "disabled" => GraphNodeState::Disabled,
                                other => {
                                    return Err(Error::new(
                                        state.span(),
                                        format!(
                                            "unknown graph node state `{other}`; expected `polling`, `interrupt`, or `disabled`"
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
                                    "unknown `graph_node` argument `{other}`; expected `graph`, `init`, `kind`, `name`, `next`, `role`, `state`, `next_node`, `sibling_of`, or `start_arc`"
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
            match args.kind.as_ref().map(ToString::to_string).as_deref() {
                Some("driver" | "internal") => {}
                Some("handoff") => {
                    return Err(Error::new(
                        Span::call_site(),
                        "handoff graph nodes require an explicit `init` function",
                    ));
                }
                _ => {
                    return Err(Error::new(
                        Span::call_site(),
                        "graph nodes require either `init` or `kind = driver|internal` for generated initialization",
                    ));
                }
            }
        } else if args.state.is_some() {
            return Err(Error::new(
                Span::call_site(),
                "`state` belongs to generated graph initialization; remove the explicit `init`",
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

fn graph_node_registration(
    name: &TokenStream2,
    next: Option<&Path>,
    sibling_of: Option<&Path>,
) -> TokenStream2 {
    match (next, sibling_of) {
        (Some(next), None) => {
            quote!(::hammer_core::data_plane::NodeRegistration::next(#name, #next::COUNT))
        }
        (None, Some(sibling_of)) => quote!(
            ::hammer_core::data_plane::NodeRegistration::sibling_of(
                #name,
                #sibling_of::NODE_NAME,
            )
        ),
        (None, None) => quote!(::hammer_core::data_plane::NodeRegistration::next(#name, 0)),
        (Some(_), Some(_)) => unreachable!("graph node args reject next with sibling_of"),
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

fn graph_node_role_from_kind(kind: Option<&Ident>) -> Result<NodeRole> {
    match kind.map(ToString::to_string).as_deref() {
        Some("driver") => Ok(NodeRole::Driver),
        Some("internal") => Ok(NodeRole::Internal),
        _ => Err(Error::new(
            Span::call_site(),
            "generated graph initialization requires `kind = driver|internal`",
        )),
    }
}

fn graph_node_state_expr(state: GraphNodeState) -> TokenStream2 {
    match state {
        GraphNodeState::Polling => quote!(::hammer_core::data_plane::NodeState::Polling),
        GraphNodeState::Interrupt => quote!(::hammer_core::data_plane::NodeState::Interrupt),
        GraphNodeState::Disabled => quote!(::hammer_core::data_plane::NodeState::Disabled),
    }
}

fn generated_graph_node_init(
    args: &GraphNodeArgs,
    ident: &Ident,
    item: &Item,
    role: NodeRole,
) -> Result<(Ident, TokenStream2)> {
    let Item::Struct(item) = item else {
        return Err(Error::new(
            item.span(),
            "graph_node can only initialize a struct",
        ));
    };
    if !matches!(item.fields, Fields::Unit) || !item.generics.params.is_empty() {
        return Err(Error::new(
            item.span(),
            "generated graph initialization requires a non-generic unit struct",
        ));
    }

    let init_ident = format_ident!(
        "__{}_graph_node_{}_init",
        args.graph.to_string().to_ascii_lowercase(),
        to_snake_case(&ident.to_string()),
    );
    let constructor = if let Some(next) = &args.next {
        quote!(#ident::new([
            ::hammer_core::data_plane::NodeId::new(0);
            #next::COUNT
        ]))
    } else {
        quote!(#ident::new())
    };
    let register = match (role, args.next.as_ref()) {
        (NodeRole::Driver, Some(next)) => quote!(
            runtime
                .nodes()
                .try_register_driver_with_next_names(node, &#next::NEXT_NAMES)?
        ),
        (NodeRole::Driver, None) => {
            quote!(runtime.nodes().try_register_driver(node)?)
        }
        (NodeRole::Internal, Some(next)) => quote!(
            runtime
                .nodes()
                .try_register_internal_with_next_names(node, &#next::NEXT_NAMES)?
        ),
        (NodeRole::Internal, None) => {
            quote!(runtime.nodes().try_register_internal(node)?)
        }
    };
    let set_state = args
        .state
        .map(graph_node_state_expr)
        .map(|state| quote!(runtime.nodes().set_node_state(node_id, #state)?;));
    let generated = quote! {
        fn #init_ident(
            runtime: &::hammer_runtime::DataPlaneRuntime,
            _: usize,
        ) -> ::hammer_core::error::CoreResult<::hammer_core::data_plane::NodeId> {
            let node = #constructor;
            let node_id = #register;
            #set_state
            Ok(node_id)
        }
    };
    Ok((init_ident, generated))
}

/// Registers a struct as a graph node via linkme `NodeEntry`.
///
/// Emits a `NodeEntry` collected into the `{graph}_GRAPH_NODES` linkme slice.
/// A zero-state `kind = driver|internal` unit node receives a generated init.
/// Nodes with business state supply `init = path`. `DataPlaneRuntime::init_graph`
/// walks the slice and resolves named next-node arcs after registration.
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
        to_snake_case(&ident.to_string()).to_ascii_uppercase()
    );
    let node_kind = graph_node_kind_expr(args.kind.as_ref());
    let node_name = if let Some(name) = &args.name {
        quote!(#name)
    } else {
        quote!(#ident::NODE_NAME)
    };
    let node_registration =
        graph_node_registration(&node_name, args.next.as_ref(), args.sibling_of.as_ref());
    let generated_role = if args.init.is_none() {
        Some(graph_node_role_from_kind(args.kind.as_ref())?)
    } else {
        None
    };
    if let (Some(declared), Some(generated)) = (args.role, generated_role)
        && declared != generated
    {
        return Err(Error::new(
            Span::call_site(),
            "graph node `role` must match `kind`",
        ));
    }
    let effective_role = generated_role.or(args.role);
    let (init, generated_init) = if let Some(init) = args.init.as_ref() {
        (quote!(#init), quote!())
    } else {
        let role = generated_role.expect("generated graph init role validated");
        let (init, generated) = generated_graph_node_init(&args, ident, &item, role)?;
        (quote!(#init), generated)
    };

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
    let needs_node_expansion = effective_role.is_some()
        || args.next_node
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
            role: effective_role,
            start_arc: args.start_arc.clone(),
        };
        let node_output = expand_node(node_args, struct_item, args.name.clone(), false)?;
        Ok(quote! {
            #node_output
            #generated_init
            #registration
        })
    } else {
        Ok(quote! {
            #item
            #generated_init
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
    fn node_function_expansion_registers_multiarch_candidates_without_graph_entry() {
        let args = syn::parse_str::<NodeFunctionArgs>("node = DeviceTxNode")
            .expect("parse Node Function args");
        let function = syn::parse_str::<ItemFn>(concat!(
            "#[cfg(feature = \"device-tx\")] ",
            "fn device_tx(runtime: &Runtime, data: Data, frame: &mut Frame) -> Result { body() }",
        ))
        .expect("parse Node Function");
        let expanded = expand_node_function(args, function).to_string();

        assert!(expanded.contains("DataPlaneInstructionSet :: Scalar"));
        assert!(expanded.contains("target_arch = \"x86_64\""));
        assert!(expanded.contains("target_feature (enable = \"avx2\")"));
        assert!(expanded.contains("target_feature (enable = \"neon\")"));
        assert!(expanded.contains("NodeFunctionRegistration :: new"));
        assert!(expanded.matches("device-tx").count() > 1);
        assert!(!expanded.contains("NodeEntry"));
    }

    #[test]
    fn node_function_rejects_instruction_set_at_call_site() {
        let error =
            match syn::parse_str::<NodeFunctionArgs>("node = DeviceTxNode, instruction_set = sse2")
            {
                Ok(_) => panic!("Node Function march variants belong to the macro"),
                Err(error) => error,
            };

        assert!(
            error
                .to_string()
                .contains("unknown `node_function` argument")
        );
    }

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
        let expanded = expand_node(args, item, None, true)
            .expect("expand node")
            .to_string();

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
        let expanded = expand_node(args, item, None, true)
            .expect("expand node")
            .to_string();

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
    fn graph_node_expands_fixed_name_driver_sibling() {
        let args = syn::parse_str::<GraphNodeArgs>(
            r#"graph = service, init = crate::register_input_sibling, name = "input-sibling", kind = driver, role = driver, sibling_of = InputOwnerNode"#,
        )
        .expect("parse graph sibling");
        let item = syn::parse_str::<Item>("pub struct InputSiblingNode;")
            .expect("parse graph sibling node");
        let ident = match &item {
            Item::Struct(item) => item.ident.clone(),
            _ => unreachable!(),
        };
        let expanded = expand_graph_node(args, &ident, item)
            .expect("expand graph sibling")
            .to_string();

        assert!(
            expanded.contains("DriverNode"),
            "missing driver role: {expanded}"
        );
        assert!(
            expanded.contains("InputOwnerNode :: NODE_NEXT_COUNT"),
            "missing inherited sibling next count: {expanded}"
        );
        assert!(
            expanded.contains("sibling_of (\"input-sibling\" , InputOwnerNode :: NODE_NAME"),
            "missing static sibling registration: {expanded}"
        );
        assert!(
            !expanded.contains("with_node_name"),
            "static graph node must not expose a name override: {expanded}"
        );
    }

    #[test]
    fn graph_node_generates_zero_state_driver_init() {
        let args = syn::parse_str::<GraphNodeArgs>(
            r#"graph = service, name = "input-owner", kind = driver, next = InputNext, state = disabled"#,
        )
        .expect("parse generated graph init");
        let item = syn::parse_str::<Item>("pub struct InputOwnerNode;")
            .expect("parse generated graph node");
        let ident = match &item {
            Item::Struct(item) => item.ident.clone(),
            _ => unreachable!(),
        };
        let expanded = expand_graph_node(args, &ident, item)
            .expect("expand generated graph init")
            .to_string();

        assert!(
            expanded.contains("DriverNode"),
            "missing driver role: {expanded}"
        );
        assert!(
            expanded.contains("try_register_driver_with_next_names"),
            "missing generated named-next registration: {expanded}"
        );
        assert!(
            expanded.contains("NodeState :: Disabled"),
            "missing generated disabled state: {expanded}"
        );
        assert!(
            expanded.contains("init : __service_graph_node_input_owner_node_init"),
            "static entry must use generated init: {expanded}"
        );
    }

    #[test]
    fn graph_node_expands_fixed_name_zero_next_internal_registration() {
        let args = syn::parse_str::<GraphNodeArgs>(
            r#"graph = service, init = crate::register_output, name = "output", kind = internal, role = internal"#,
        )
        .expect("parse zero-next graph node");
        let item = syn::parse_str::<Item>("pub struct OutputNode;")
            .expect("parse zero-next graph node item");
        let ident = match &item {
            Item::Struct(item) => item.ident.clone(),
            _ => unreachable!(),
        };
        let expanded = expand_graph_node(args, &ident, item)
            .expect("expand zero-next graph node")
            .to_string();

        assert!(
            expanded.contains("NodeRegistration :: next (Self :: NODE_NAME , 0"),
            "zero-next graph node must keep its fixed name: {expanded}"
        );
        assert!(
            !expanded.contains("NodeRegistration :: Plain"),
            "zero-next graph node must not register anonymously: {expanded}"
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
        assert!(
            expanded.contains("fn slot (self) -> u16"),
            "NodeNext::slot must return u16: {expanded}"
        );
        assert!(
            expanded.contains("pub const fn slot (self) -> usize"),
            "generated enums keep inherent usize slot metadata: {expanded}"
        );
        assert!(
            !expanded.contains("const COUNT : usize = SampleNext :: COUNT"),
            "NodeNext trait must not require COUNT: {expanded}"
        );
        assert!(
            !expanded.contains("MAX_NODE_NEXT_SLOTS"),
            "obsolete 16-next macro guard must be gone: {expanded}"
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
