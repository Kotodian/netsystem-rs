use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Error, Expr, Field, Fields, FieldsNamed, FnArg, GenericArgument, GenericParam,
    Generics, Ident, Item, ItemEnum, ItemFn, ItemMod, ItemStruct, LitBool, LitStr, Path,
    PathArguments, Result, ReturnType, Token, Type, bracketed, parse_macro_input, parse_quote,
    spanned::Spanned,
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
/// The function is compiled once per supported SIMD width. A function may
/// declare one `const SIMD_BYTES: usize` generic to receive that width.
/// Conditional compilation attributes on the function gate every generated
/// variant.
#[proc_macro_attribute]
pub fn node_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as NodeFunctionArgs);
    let function = parse_macro_input!(input as ItemFn);
    expand_node_function(args, function).into()
}

fn expand_node_function(args: NodeFunctionArgs, function: ItemFn) -> TokenStream2 {
    let node = &args.node;
    let function_name = function.sig.ident.clone();
    let has_simd_generic = match function.sig.generics.params.len() {
        0 => false,
        1 => match function.sig.generics.params.first() {
            Some(GenericParam::Const(_)) => true,
            _ => {
                return Error::new_spanned(
                    &function.sig.generics,
                    "`node_function` only accepts a const SIMD-width generic",
                )
                .to_compile_error();
            }
        },
        _ => {
            return Error::new_spanned(
                &function.sig.generics,
                "`node_function` accepts at most one const SIMD-width generic",
            )
            .to_compile_error();
        }
    };
    if has_simd_generic
        && !matches!(
            function.sig.generics.params.first(),
            Some(GenericParam::Const(parameter))
                if matches!(&parameter.ty, Type::Path(path) if path.path.is_ident("usize"))
        )
    {
        return Error::new_spanned(
            &function.sig.generics,
            "the `node_function` SIMD-width generic must be `const ...: usize`",
        )
        .to_compile_error();
    }
    // VPP recompiles one VLIB_NODE_FN body for each enabled march variant.
    // Generate the equivalent private symbols from one Rust declaration.
    let scalar = expand_node_function_variant(
        node,
        &function,
        function_name,
        "scalar",
        1,
        has_simd_generic,
        quote!(),
    );
    let simd128 = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_simd128", function.sig.ident),
        "simd128",
        16,
        has_simd_generic,
        quote!(
            #[cfg_attr(any(target_arch = "x86", target_arch = "x86_64"), target_feature(enable = "sse2"))]
            #[cfg_attr(any(target_arch = "arm", target_arch = "aarch64"), target_feature(enable = "neon"))]
        ),
    );
    let simd256 = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_simd256", function.sig.ident),
        "simd256",
        32,
        has_simd_generic,
        quote!(#[cfg_attr(any(target_arch = "x86", target_arch = "x86_64"), target_feature(enable = "avx2"))]),
    );
    let simd512 = expand_node_function_variant(
        node,
        &function,
        format_ident!("__{}_simd512", function.sig.ident),
        "simd512",
        64,
        has_simd_generic,
        quote!(#[cfg_attr(any(target_arch = "x86", target_arch = "x86_64"), target_feature(enable = "avx512f,avx512bw"))]),
    );

    quote! {
        #scalar
        #simd128
        #simd256
        #simd512
    }
}

fn expand_node_function_variant(
    node: &Path,
    function: &ItemFn,
    function_name: Ident,
    suffix: &str,
    simd_bytes: usize,
    has_simd_generic: bool,
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
    let registered_function = if has_simd_generic {
        quote!(#function_name::<#simd_bytes>)
    } else {
        quote!(#function_name)
    };
    quote! {
        #target_feature
        #variant_function

        #(#input_cfg)*
        pub(crate) static #static_name: ::hammer_runtime::node::NodeFunctionRegistration =
            unsafe {
                ::hammer_runtime::node::NodeFunctionRegistration::new(
                #node::NODE_NAME,
                ::hammer_runtime::Simd::<u8, #simd_bytes>::splat(0),
                #registered_function,
                )
            };
    }
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
    expand_node(args, item, None, true, true)
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
    store_initial_nexts: bool,
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
            "`node(next = ...)` owns the `next` field; remove it from the struct",
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
        if store_initial_nexts {
            let field: Field = parse_quote! {
                next: [::hammer_core::data_plane::NodeId; #next::COUNT]
            };
            output_fields.push(field);
            constructor_params
                .push(quote!(next: [::hammer_core::data_plane::NodeId; #next::COUNT]));
            constructor_inits.push(quote!(next));
        }
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
    let initial_nexts_inherent_impl =
        if args.role.is_some() && args.next.is_some() && store_initial_nexts {
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
            let initial_nexts = if args.next.is_some() && store_initial_nexts {
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
            let initial_nexts = if args.next.is_some() && store_initial_nexts {
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
    /// Deprecated: ignored for slice targeting; kept for source/symbol naming compat.
    graph: Option<Ident>,
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
            graph: None,
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
                        "graph" => args.graph = Some(input.parse()?),
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
                                    "unknown `graph_node` argument `{other}`; expected `graph`, `init`, `plugin`, `kind`, `name`, `next`, `role`, `state`, `next_node`, `sibling_of`, or `start_arc`"
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

    let graph_label = args
        .graph
        .as_ref()
        .map(|graph| graph.to_string().to_ascii_lowercase())
        .unwrap_or_else(|| "graph".to_owned());
    let init_ident = format_ident!(
        "__{}_graph_node_{}_init",
        graph_label,
        to_snake_case(&ident.to_string()),
    );
    let constructor = quote!(#ident::new());
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
        ) -> ::hammer_runtime::RuntimeResult<::hammer_core::data_plane::NodeId> {
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
/// Emits a `NodeEntry` into the current link image's private catalog.
/// A zero-state `kind = driver|internal` unit node receives a generated init.
/// Nodes with business state supply `init = path`. `DataPlaneRuntime::init_graph`
/// walks the filtered catalog and resolves named next-node arcs after registration.
/// ```ignore
/// #[hammer_component_macros::graph_node(kind = driver, name = "device-input", next = DeviceInputNext)]
/// pub struct DeviceInputNode;
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

struct AppSessionProtocolArgs {
    name: LitStr,
    lower: LitStr,
    upper: LitStr,
}

impl Parse for AppSessionProtocolArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut lower = None;
        let mut upper = None;
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
                "lower" => {
                    if lower.is_some() {
                        return Err(Error::new(key.span(), "duplicate `lower` argument"));
                    }
                    lower = Some(input.parse()?);
                }
                "upper" => {
                    if upper.is_some() {
                        return Err(Error::new(key.span(), "duplicate `upper` argument"));
                    }
                    upper = Some(input.parse()?);
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown `app_session_protocol` argument `{other}`; expected `name`, `lower`, or `upper`"
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
            lower: lower
                .ok_or_else(|| Error::new(Span::call_site(), "missing `lower` argument"))?,
            upper: upper
                .ok_or_else(|| Error::new(Span::call_site(), "missing `upper` argument"))?,
        })
    }
}

/// Registers a concrete App Session protocol in the current link image.
#[proc_macro_attribute]
pub fn app_session_protocol(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AppSessionProtocolArgs);
    let item = parse_macro_input!(input as Item);
    let ident = match item {
        Item::Struct(ref item) => item.ident.clone(),
        _ => {
            return Error::new(
                item.span(),
                "`app_session_protocol` can only be attached to a struct",
            )
            .to_compile_error()
            .into();
        }
    };
    let static_ident = format_ident!(
        "__APP_SESSION_PROTOCOL_{}",
        to_snake_case(&ident.to_string()).to_ascii_uppercase()
    );
    let connections_ident = format_ident!("{static_ident}_CONNECTIONS");
    let create_ident = format_ident!("{static_ident}_CREATE");
    let ingress_ident = format_ident!("{static_ident}_INGRESS");
    let egress_ident = format_ident!("{static_ident}_EGRESS");
    let destroy_ident = format_ident!("{static_ident}_DESTROY");
    let name = args.name;
    let lower = args.lower;
    let upper = args.upper;
    quote! {
        #item

        static #connections_ident: ::std::sync::OnceLock<
            ::hammer_runtime::app::AppSessionProtocolConnections<#ident>
        > = ::std::sync::OnceLock::new();

        fn #create_ident(
            __hammer_worker: ::hammer_runtime::DataWorkerId,
            __hammer_worker_count: usize,
            __hammer_application: ::std::option::Option<::hammer_runtime::app::ApplicationId>,
            __hammer_role: ::hammer_runtime::app::AppSessionProtocolRole,
            __hammer_protocol_id: ::std::option::Option<u64>,
            __hammer_server_name: ::std::option::Option<&str>,
        ) -> ::hammer_runtime::RuntimeResult<
            ::hammer_runtime::app::AppSessionProtocolConnectionId
        > {
            let __hammer_protocol = <#ident as ::hammer_runtime::app::AppSessionProtocol>::create(
                __hammer_application,
                __hammer_role,
                __hammer_protocol_id,
                __hammer_server_name,
            )?;
            #connections_ident.get_or_init(|| {
                ::hammer_runtime::app::AppSessionProtocolConnections::new(
                    __hammer_worker_count,
                    <#ident as ::hammer_runtime::app::AppSessionProtocol>::CONNECTION_CAPACITY,
                )
            }).insert(__hammer_worker, __hammer_worker_count, __hammer_protocol)
        }

        fn #ingress_ident(
            __hammer_worker: ::hammer_runtime::DataWorkerId,
            __hammer_connection: ::hammer_runtime::app::AppSessionProtocolConnectionId,
            __hammer_lower_rx_fifo: &::hammer_infra::fifo::Fifo,
            __hammer_upper_rx_fifo: &::hammer_infra::fifo::Fifo,
        ) -> ::hammer_runtime::RuntimeResult<(usize, usize)> {
            #connections_ident.get()
                .expect("App Session protocol connection storage exists after construction")
                .with_mut(__hammer_worker, __hammer_connection, |__hammer_protocol| {
                    <#ident as ::hammer_runtime::app::AppSessionProtocol>::ingress(
                        __hammer_protocol,
                        __hammer_lower_rx_fifo,
                        __hammer_upper_rx_fifo,
                    )
                })
        }

        fn #egress_ident(
            __hammer_worker: ::hammer_runtime::DataWorkerId,
            __hammer_connection: ::hammer_runtime::app::AppSessionProtocolConnectionId,
            __hammer_upper_tx_fifo: &::hammer_infra::fifo::Fifo,
            __hammer_lower_tx_fifo: &::hammer_infra::fifo::Fifo,
        ) -> ::hammer_runtime::RuntimeResult<(usize, usize)> {
            #connections_ident.get()
                .expect("App Session protocol connection storage exists after construction")
                .with_mut(__hammer_worker, __hammer_connection, |__hammer_protocol| {
                    <#ident as ::hammer_runtime::app::AppSessionProtocol>::egress(
                        __hammer_protocol,
                        __hammer_upper_tx_fifo,
                        __hammer_lower_tx_fifo,
                    )
                })
        }

        fn #destroy_ident(
            __hammer_worker: ::hammer_runtime::DataWorkerId,
            __hammer_connection: ::hammer_runtime::app::AppSessionProtocolConnectionId,
        ) -> ::hammer_runtime::RuntimeResult<()> {
            #connections_ident.get()
                .expect("App Session protocol connection storage exists after construction")
                .remove(__hammer_worker, __hammer_connection)
        }

        pub(crate) static #static_ident: ::hammer_runtime::app::AppSessionProtocolEntry =
            ::hammer_runtime::app::AppSessionProtocolEntry::new(
                #name,
                #lower,
                #upper,
                #create_ident,
                #ingress_ident,
                #egress_ident,
                #destroy_ident,
            );
    }
    .into()
}

struct SessionTransportArgs {
    name: LitStr,
    upper: LitStr,
}

impl Parse for SessionTransportArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut upper = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                "upper" => upper = Some(input.parse()?),
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown `session_transport` argument `{other}`; expected `name` or `upper`"
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
            upper: upper
                .ok_or_else(|| Error::new(Span::call_site(), "missing `upper` argument"))?,
        })
    }
}

/// Registers one Session Transport's upper protocol semantics.
#[proc_macro_attribute]
pub fn session_transport(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as SessionTransportArgs);
    let item = parse_macro_input!(input as Item);
    let ident = match item {
        Item::Struct(ref item) => item.ident.clone(),
        _ => {
            return Error::new(
                item.span(),
                "`session_transport` can only be attached to a struct",
            )
            .to_compile_error()
            .into();
        }
    };
    let static_ident = format_ident!(
        "__SESSION_TRANSPORT_{}",
        to_snake_case(&ident.to_string()).to_ascii_uppercase()
    );
    let name = args.name;
    let upper = args.upper;
    quote! {
        #item

        pub(crate) static #static_ident: ::hammer_runtime::app::SessionTransportRegistration =
            ::hammer_runtime::app::SessionTransportRegistration::new(#name, #upper);
    }
    .into()
}

struct BinaryApiArgs {
    name: LitStr,
}

impl Parse for BinaryApiArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let key: Ident = input.parse()?;
        if key != "name" {
            return Err(Error::new(key.span(), "expected `name`"));
        }
        input.parse::<Token![=]>()?;
        let name: LitStr = input.parse()?;
        if name.value().trim().is_empty() {
            return Err(Error::new(name.span(), "Binary API method name is empty"));
        }
        if input.parse::<Option<Token![,]>>()?.is_some() || !input.is_empty() {
            return Err(Error::new(input.span(), "unexpected Binary API argument"));
        }
        Ok(Self { name })
    }
}

/// Registers one protobuf request/reply handler in the current link image.
#[proc_macro_attribute]
pub fn binary_api(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as BinaryApiArgs);
    let function = parse_macro_input!(input as ItemFn);
    expand_binary_api(args, function)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_binary_api(args: BinaryApiArgs, function: ItemFn) -> Result<TokenStream2> {
    if function.sig.asyncness.is_some()
        || function.sig.unsafety.is_some()
        || function.sig.abi.is_some()
        || function.sig.variadic.is_some()
        || !function.sig.generics.params.is_empty()
    {
        return Err(Error::new_spanned(
            &function.sig,
            "Binary API handlers must be synchronous, safe, non-generic Rust functions",
        ));
    }
    let inputs = function.sig.inputs.iter().collect::<Vec<_>>();
    let Some(FnArg::Typed(request)) = inputs.first().copied() else {
        return Err(Error::new_spanned(
            &function.sig.inputs,
            "Binary API handlers must accept a protobuf request by value",
        ));
    };
    if inputs.len() > 2 || inputs.is_empty() {
        return Err(Error::new_spanned(
            &function.sig.inputs,
            "Binary API handlers accept one request and an optional &mut BinaryApiContext",
        ));
    }
    if !request.attrs.is_empty() {
        return Err(Error::new_spanned(
            &request.attrs[0],
            "Binary API request parameters do not accept attributes",
        ));
    }
    let function_name = &function.sig.ident;
    let call = match inputs.as_slice() {
        [_] => quote!(#function_name(__hammer_request)),
        [_, FnArg::Typed(context)]
            if matches!(context.ty.as_ref(), Type::Reference(reference)
                if reference.mutability.is_some()
                    && type_path_ends_with(&reference.elem, "BinaryApiContext")) =>
        {
            quote!(#function_name(__hammer_request, __hammer_context))
        }
        [_, second] => {
            return Err(Error::new_spanned(
                second,
                "the second Binary API parameter must be &mut BinaryApiContext",
            ));
        }
        _ => unreachable!("Binary API input count was validated"),
    };
    let ReturnType::Type(_, reply_ty) = &function.sig.output else {
        return Err(Error::new_spanned(
            &function.sig.output,
            "Binary API handlers must return one protobuf reply",
        ));
    };
    let request_ty = &request.ty;
    let adapter_name = format_ident!("__hammer_binary_api_adapter_{}", function_name);
    let static_name = format_ident!(
        "__BINARY_API_{}",
        to_snake_case(&function_name.to_string()).to_ascii_uppercase()
    );
    let name = args.name;
    let conditional_attributes: Vec<_> = function
        .attrs
        .iter()
        .filter(|attribute| {
            attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
        })
        .cloned()
        .collect();

    Ok(quote! {
        #function

        #(#conditional_attributes)*
        fn #adapter_name(
            __hammer_request: ::hammer_runtime::__private::RSlice<'_, u8>,
            __hammer_context: &mut ::hammer_runtime::__private::BinaryApiContext,
        ) -> ::hammer_runtime::__private::BinaryApiMethodReply {
            let __hammer_request = match <#request_ty as ::prost::Message>::decode(
                __hammer_request.as_slice(),
            ) {
                Ok(request) => request,
                Err(_) => {
                    return ::hammer_runtime::__private::BinaryApiMethodReply::invalid_request();
                }
            };
            match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| {
                let __hammer_reply: #reply_ty = #call;
                <#reply_ty as ::prost::Message>::encode_to_vec(&__hammer_reply)
            })) {
                Ok(payload) => ::hammer_runtime::__private::BinaryApiMethodReply::ok(payload),
                Err(_) => ::hammer_runtime::__private::BinaryApiMethodReply::panicked(),
            }
        }

        #(#conditional_attributes)*
        pub(crate) static #static_name: ::hammer_runtime::__private::BinaryApiMethodEntry =
            ::hammer_runtime::__private::BinaryApiMethodEntry::new(#name, #adapter_name);
    })
}

fn expand_graph_node(args: GraphNodeArgs, ident: &Ident, item: Item) -> Result<TokenStream2> {
    let graph_label = args
        .graph
        .as_ref()
        .map(|graph| graph.to_string().to_ascii_uppercase())
        .unwrap_or_else(|| "GRAPH".to_owned());
    let static_ident = format_ident!(
        "__{}_GRAPH_NODE_{}",
        graph_label,
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
        pub(crate) static #static_ident: ::hammer_runtime::NodeEntry =
            ::hammer_runtime::NodeEntry {
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
        let node_output = expand_node(node_args, struct_item, args.name.clone(), false, false)?;
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

struct ProcessFnArgs {
    name: LitStr,
}

impl Parse for ProcessFnArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!("unknown Process Node argument `{other}`; expected `name`"),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        Ok(Self {
            name: name.ok_or_else(|| Error::new(Span::call_site(), "missing `name` argument"))?,
        })
    }
}

/// Registers an async VPP-style Process Node on the main-thread executor.
#[proc_macro_attribute]
pub fn process_node(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ProcessFnArgs);
    let function = parse_macro_input!(input as ItemFn);
    expand_process_node(args, function)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_process_node(args: ProcessFnArgs, function: ItemFn) -> Result<TokenStream2> {
    let signature = &function.sig;
    if signature.asyncness.is_none()
        || signature.constness.is_some()
        || signature.unsafety.is_some()
        || signature.abi.is_some()
        || signature.variadic.is_some()
        || !signature.generics.params.is_empty()
        || signature.generics.where_clause.is_some()
    {
        return Err(Error::new(
            signature.span(),
            "Process Nodes must be safe, async, non-generic Rust functions",
        ));
    }
    if signature.inputs.len() != 1 {
        return Err(Error::new(
            signature.inputs.span(),
            "Process Nodes take exactly one ProcessContext parameter",
        ));
    }
    let Some(FnArg::Typed(context)) = signature.inputs.first() else {
        return Err(Error::new(
            signature.inputs.span(),
            "Process Nodes cannot have a receiver",
        ));
    };
    if !type_path_ends_with(&context.ty, "ProcessContext") {
        return Err(Error::new(
            context.ty.span(),
            "Process Node parameter must be ProcessContext",
        ));
    }
    let ReturnType::Type(_, output) = &signature.output else {
        return Err(Error::new(
            signature.output.span(),
            "Process Nodes must return RuntimeResult<()> through their future",
        ));
    };
    let Some(value) = wrapped_type(output, "RuntimeResult") else {
        return Err(Error::new(
            output.span(),
            "Process Nodes must return RuntimeResult<()> through their future",
        ));
    };
    if !matches!(&value, Type::Tuple(tuple) if tuple.elems.is_empty()) {
        return Err(Error::new(
            value.span(),
            "Process Nodes must return RuntimeResult<()> through their future",
        ));
    }

    let function_name = &function.sig.ident;
    let adapter_name = format_ident!("__hammer_process_adapter_{}", function_name);
    let static_ident = format_ident!(
        "__PROCESS_NODE_{}",
        args.name.value().to_ascii_uppercase().replace('-', "_")
    );
    let name = args.name;
    let conditional_attributes: Vec<_> = function
        .attrs
        .iter()
        .filter(|attribute| {
            attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
        })
        .cloned()
        .collect();

    Ok(quote! {
        #function

        #(#conditional_attributes)*
        fn #adapter_name(
            __hammer_context: ::hammer_runtime::ProcessContext,
        ) -> ::hammer_runtime::ProcessFuture {
            ::std::boxed::Box::pin(#function_name(__hammer_context))
        }

        #(#conditional_attributes)*
        pub(crate) static #static_ident: ::hammer_runtime::ProcessEntry =
            ::hammer_runtime::ProcessEntry {
            name: #name,
            start: #adapter_name,
        };
    })
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
    init: InitFnArgs,
    section: LitStr,
    early: bool,
}

impl Parse for ConfigFnArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut section = None;
        let mut early = None;
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
                "section" => {
                    if section.is_some() {
                        return Err(Error::new(key.span(), "duplicate `section` argument"));
                    }
                    section = Some(input.parse()?);
                }
                "runs_before" => runs_before = parse_litstr_array(input)?,
                "runs_after" => runs_after = parse_litstr_array(input)?,
                "early" => {
                    if early.is_some() {
                        return Err(Error::new(key.span(), "duplicate `early` argument"));
                    }
                    early = Some(input.parse::<LitBool>()?.value());
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown argument `{other}`; expected `name`, `section`, `early`, `runs_before`, or `runs_after`"
                        ),
                    ));
                }
            }
            if input.parse::<Option<Token![,]>>()?.is_none() {
                break;
            }
        }
        Ok(Self {
            init: InitFnArgs {
                name: name
                    .ok_or_else(|| Error::new(Span::call_site(), "missing `name` argument"))?,
                runs_before,
                runs_after,
            },
            section: section
                .ok_or_else(|| Error::new(Span::call_site(), "missing `section` argument"))?,
            early: early.unwrap_or(false),
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

fn parse_path_array(input: ParseStream<'_>) -> Result<Vec<Path>> {
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

fn config_function_static_name(fn_name: &LitStr) -> Ident {
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
    format_ident!("__CONFIG_FN_{}", sanitized.to_ascii_uppercase())
}

/// Registers a function as an init function in the topologically-sorted init chain.
///
/// Example:
/// ```ignore
/// #[init_function(name = "tcp_init", runs_after = ["buffer_main_init"], runs_before = ["session_init"])]
/// fn tcp_init(vm: &mut Engine, config: Arc<Config>) -> RuntimeResult<Arc<TcpMain>> { ... }
/// ```
#[proc_macro_attribute]
pub fn init_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as InitFnArgs);
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_registered_function(args, fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

enum InitArgument {
    Engine,
    Required { binding: Ident, ty: Type },
    Optional { binding: Ident, ty: Type },
}

enum InitOutput {
    Unit,
    Arc,
    OptionalArc,
}

fn expand_registered_function(args: InitFnArgs, mut function: ItemFn) -> Result<TokenStream2> {
    validate_init_function_qualifiers(&function)?;
    let arguments = init_arguments(&mut function)?;
    let output = init_output(&function)?;
    let function_name = &function.sig.ident;
    let adapter_name = format_ident!("__hammer_init_adapter_{}", function_name);
    let name = args.name;
    let runs_before = args.runs_before;
    let runs_after = args.runs_after;
    let static_ident = init_function_static_name(&name);
    let conditional_attributes: Vec<_> = function
        .attrs
        .iter()
        .filter(|attribute| {
            attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
        })
        .cloned()
        .collect();
    let adapter_attributes = conditional_attributes.clone();
    let registration_attributes = conditional_attributes;
    let mut injections = Vec::new();
    let mut call_arguments = Vec::with_capacity(arguments.len());
    for argument in arguments {
        match argument {
            InitArgument::Engine => call_arguments.push(quote!(__hammer_engine)),
            InitArgument::Required { binding, ty } => {
                injections.push(quote! {
                    let #binding = __hammer_engine.registry.require::<#ty>()?;
                });
                call_arguments.push(quote!(#binding));
            }
            InitArgument::Optional { binding, ty } => {
                injections.push(quote! {
                    let Some(#binding) = __hammer_engine.registry.get::<#ty>() else {
                        return Ok(());
                    };
                });
                call_arguments.push(quote!(#binding));
            }
        }
    }
    let invoke = match output {
        InitOutput::Unit => quote!(#function_name(#(#call_arguments),*)),
        InitOutput::Arc => quote! {
            let __hammer_produced = #function_name(#(#call_arguments),*)?;
            __hammer_engine.registry.set(__hammer_produced);
            Ok(())
        },
        InitOutput::OptionalArc => quote! {
            if let Some(__hammer_produced) = #function_name(#(#call_arguments),*)? {
                __hammer_engine.registry.set(__hammer_produced);
            }
            Ok(())
        },
    };

    Ok(quote! {
        #function

        #(#adapter_attributes)*
        fn #adapter_name(
            __hammer_engine: &mut ::hammer_runtime::Engine,
        ) -> ::hammer_runtime::RuntimeResult<()> {
            #(#injections)*
            #invoke
        }

        #(#registration_attributes)*
        pub(crate) static #static_ident: ::hammer_runtime::init::InitFunction =
            ::hammer_runtime::init::InitFunction {
            name: #name,
            runs_before: &[#(#runs_before),*],
            runs_after: &[#(#runs_after),*],
            func: #adapter_name,
        };
    })
}

fn validate_init_function_qualifiers(function: &ItemFn) -> Result<()> {
    let signature = &function.sig;
    if signature.constness.is_some()
        || signature.asyncness.is_some()
        || signature.unsafety.is_some()
        || signature.abi.is_some()
        || signature.variadic.is_some()
        || !signature.generics.params.is_empty()
        || signature.generics.where_clause.is_some()
    {
        return Err(Error::new(
            signature.span(),
            "init functions must be safe, synchronous, non-generic Rust functions",
        ));
    }
    Ok(())
}

fn init_arguments(function: &mut ItemFn) -> Result<Vec<InitArgument>> {
    let mut arguments = Vec::with_capacity(function.sig.inputs.len());
    let mut engine_count = 0usize;
    for (index, argument) in function.sig.inputs.iter_mut().enumerate() {
        let FnArg::Typed(argument) = argument else {
            return Err(Error::new(
                argument.span(),
                "init functions cannot have a receiver",
            ));
        };
        let optional = take_optional_injection(&mut argument.attrs)?;
        if is_mut_engine_reference(&argument.ty) {
            if optional {
                return Err(Error::new(
                    argument.span(),
                    "the Engine parameter cannot use #[inject(optional)]",
                ));
            }
            engine_count += 1;
            arguments.push(InitArgument::Engine);
            continue;
        }
        let Some(ty) = wrapped_type(&argument.ty, "Arc") else {
            return Err(Error::new(
                argument.ty.span(),
                "init parameters must be `&mut Engine` or `Arc<T>`",
            ));
        };
        let binding = format_ident!("__hammer_injected_{index}");
        arguments.push(if optional {
            InitArgument::Optional { binding, ty }
        } else {
            InitArgument::Required { binding, ty }
        });
    }
    if engine_count > 1 {
        return Err(Error::new(
            function.sig.inputs.span(),
            "init functions can have at most one `&mut Engine` parameter",
        ));
    }
    Ok(arguments)
}

fn take_optional_injection(attributes: &mut Vec<Attribute>) -> Result<bool> {
    let mut optional = false;
    let mut retained = Vec::with_capacity(attributes.len());
    for attribute in attributes.drain(..) {
        if !attribute.path().is_ident("inject") {
            retained.push(attribute);
            continue;
        }
        if optional {
            return Err(Error::new(
                attribute.span(),
                "duplicate #[inject(optional)] attribute",
            ));
        }
        let mode = attribute.parse_args::<Ident>()?;
        if mode != "optional" {
            return Err(Error::new(mode.span(), "expected `inject(optional)`"));
        }
        optional = true;
    }
    *attributes = retained;
    Ok(optional)
}

fn is_mut_engine_reference(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    reference.mutability.is_some() && type_path_ends_with(&reference.elem, "Engine")
}

fn type_path_ends_with(ty: &Type, expected: &str) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.qself.is_none()
        && path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == expected)
}

fn wrapped_type(ty: &Type, wrapper: &str) -> Option<Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if path.qself.is_some() || segment.ident != wrapper {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    match arguments.args.first()? {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    }
}

fn init_output(function: &ItemFn) -> Result<InitOutput> {
    let ReturnType::Type(_, result) = &function.sig.output else {
        return Err(Error::new(
            function.sig.output.span(),
            "init functions must return RuntimeResult<T>",
        ));
    };
    let Some(value) = wrapped_type(result, "RuntimeResult") else {
        return Err(Error::new(
            result.span(),
            "init functions must return RuntimeResult<T>",
        ));
    };
    if matches!(&value, Type::Tuple(tuple) if tuple.elems.is_empty()) {
        return Ok(InitOutput::Unit);
    }
    if wrapped_type(&value, "Arc").is_some() {
        return Ok(InitOutput::Arc);
    }
    if let Some(value) = wrapped_type(&value, "Option")
        && wrapped_type(&value, "Arc").is_some()
    {
        return Ok(InitOutput::OptionalArc);
    }
    Err(Error::new(
        value.span(),
        "init functions must return RuntimeResult<()>, RuntimeResult<Arc<T>>, or RuntimeResult<Option<Arc<T>>>",
    ))
}

/// Registers a section-scoped serde config provider in the ordered config phase.
///
/// Example:
/// ```ignore
/// #[config_function(name = "tcp_config", section = "plugin.tcp", early = true)]
/// fn configure_tcp(config: TcpPluginConfig, engine: &mut Engine) -> RuntimeResult<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn config_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ConfigFnArgs);
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_config_function(args, fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

enum ConfigArgument {
    Section { ty: Type },
    Engine,
    Required { binding: Ident, ty: Type },
    Optional { binding: Ident, ty: Type },
}

fn config_arguments(function: &mut ItemFn) -> Result<Vec<ConfigArgument>> {
    let mut arguments = Vec::with_capacity(function.sig.inputs.len());
    let mut section_count = 0usize;
    let mut engine_count = 0usize;
    for (index, argument) in function.sig.inputs.iter_mut().enumerate() {
        let FnArg::Typed(argument) = argument else {
            return Err(Error::new(
                argument.span(),
                "config functions cannot have a receiver",
            ));
        };
        let optional = take_optional_injection(&mut argument.attrs)?;
        if is_mut_engine_reference(&argument.ty) {
            if optional {
                return Err(Error::new(
                    argument.span(),
                    "the Engine parameter cannot use #[inject(optional)]",
                ));
            }
            engine_count += 1;
            arguments.push(ConfigArgument::Engine);
            continue;
        }
        if let Some(ty) = wrapped_type(&argument.ty, "Arc") {
            let binding = format_ident!("__hammer_injected_{index}");
            arguments.push(if optional {
                ConfigArgument::Optional { binding, ty }
            } else {
                ConfigArgument::Required { binding, ty }
            });
            continue;
        }
        if optional {
            return Err(Error::new(
                argument.span(),
                "only injected Arc<T> parameters can use #[inject(optional)]",
            ));
        }
        section_count += 1;
        arguments.push(ConfigArgument::Section {
            ty: (*argument.ty).clone(),
        });
    }
    if section_count != 1 {
        return Err(Error::new(
            function.sig.inputs.span(),
            "config functions must have exactly one by-value serde config parameter",
        ));
    }
    if engine_count > 1 {
        return Err(Error::new(
            function.sig.inputs.span(),
            "config functions can have at most one `&mut Engine` parameter",
        ));
    }
    Ok(arguments)
}

fn expand_config_function(args: ConfigFnArgs, mut function: ItemFn) -> Result<TokenStream2> {
    validate_init_function_qualifiers(&function)?;
    let arguments = config_arguments(&mut function)?;
    let output = init_output(&function)?;
    let function_name = &function.sig.ident;
    let adapter_name = format_ident!("__hammer_config_adapter_{}", function_name);
    let name = args.init.name;
    let section = args.section;
    let runs_before = args.init.runs_before;
    let runs_after = args.init.runs_after;
    let static_ident = config_function_static_name(&name);
    let conditional_attributes: Vec<_> = function
        .attrs
        .iter()
        .filter(|attribute| {
            attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
        })
        .cloned()
        .collect();
    let adapter_attributes = conditional_attributes.clone();
    let registration_attributes = conditional_attributes;
    let section_keys: Vec<LitStr> = section
        .value()
        .split('.')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(|key| LitStr::new(key, section.span()))
        .collect();
    if section_keys.is_empty() {
        return Err(Error::new(
            section.span(),
            "config section must not be empty",
        ));
    }
    let mut injections = Vec::new();
    let mut call_arguments = Vec::with_capacity(arguments.len());
    for argument in arguments {
        match argument {
            ConfigArgument::Section { ty } => {
                let wrapper_idents: Vec<_> = section_keys
                    .iter()
                    .enumerate()
                    .map(|(index, _)| {
                        format_ident!("__HammerConfigPath_{}_{}", function_name, index)
                    })
                    .collect();
                let field_idents: Vec<_> = section_keys
                    .iter()
                    .enumerate()
                    .map(|(index, _)| format_ident!("value_{index}"))
                    .collect();
                let mut wrapper_definitions = Vec::with_capacity(section_keys.len());
                let mut child = quote!(#ty);
                for index in (0..section_keys.len()).rev() {
                    let wrapper = &wrapper_idents[index];
                    let field = &field_idents[index];
                    let key = &section_keys[index];
                    wrapper_definitions.push(quote! {
                        #[allow(non_camel_case_types)]
                        #[derive(::serde::Deserialize, Default)]
                        #[serde(default)]
                        struct #wrapper {
                            #[serde(default, rename = #key)]
                            #field: #child,
                        }
                    });
                    child = quote!(#wrapper);
                }
                let root = &wrapper_idents[0];
                let value = field_idents.iter().fold(
                    quote!(__hammer_config_document),
                    |value, field| quote!(#value.#field),
                );
                injections.push(quote! {
                    #(#wrapper_definitions)*
                    let __hammer_config_document: #root = ::toml::from_str(__hammer_document)
                        .map_err(|error| ::hammer_runtime::RuntimeError::config_parse(format!(
                            "config function `{}` section `{}`: {error}",
                            #name,
                            #section,
                        )))?;
                    let __hammer_config: #ty =
                        #value;
                });
                call_arguments.push(quote!(__hammer_config));
            }
            ConfigArgument::Engine => call_arguments.push(quote!(__hammer_engine)),
            ConfigArgument::Required { binding, ty } => {
                injections.push(quote! {
                    let #binding = __hammer_engine.registry.require::<#ty>()?;
                });
                call_arguments.push(quote!(#binding));
            }
            ConfigArgument::Optional { binding, ty } => {
                injections.push(quote! {
                    let Some(#binding) = __hammer_engine.registry.get::<#ty>() else {
                        return Ok(());
                    };
                });
                call_arguments.push(quote!(#binding));
            }
        }
    }
    let invoke = match output {
        InitOutput::Unit => quote!(#function_name(#(#call_arguments),*)),
        InitOutput::Arc => quote! {
            let __hammer_produced = #function_name(#(#call_arguments),*)?;
            __hammer_engine.registry.set(__hammer_produced);
            Ok(())
        },
        InitOutput::OptionalArc => quote! {
            if let Some(__hammer_produced) = #function_name(#(#call_arguments),*)? {
                __hammer_engine.registry.set(__hammer_produced);
            }
            Ok(())
        },
    };

    Ok(quote! {
        #function

        #(#adapter_attributes)*
        fn #adapter_name(
            __hammer_document: &str,
            __hammer_engine: &mut ::hammer_runtime::Engine,
        ) -> ::hammer_runtime::RuntimeResult<()> {
            #(#injections)*
            #invoke
        }

        #(#registration_attributes)*
        pub(crate) static #static_ident: ::hammer_runtime::init::ConfigFunction =
            ::hammer_runtime::init::ConfigFunction {
                name: #name,
                section: #section,
                runs_before: &[#(#runs_before),*],
                runs_after: &[#(#runs_after),*],
                func: #adapter_name,
            };
    })
}

/// Shorthand for `#[config_function(name = "...", section = "...", early = true)]`.
#[proc_macro_attribute]
pub fn early_config_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut args = parse_macro_input!(args as ConfigFnArgs);
    args.early = true;
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_config_function(args, fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Registers a function to run at main-loop-enter time (e.g., `start_workers`).
///
/// Example:
/// ```ignore
/// #[main_loop_enter_function]
/// fn start_workers(vm: &mut Engine, config: Arc<Config>) -> RuntimeResult<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn main_loop_enter_function(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(
            Span::call_site(),
            "main_loop_enter_function takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_main_loop_function(fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Registers a function to run at main-loop-exit time.
#[proc_macro_attribute]
pub fn main_loop_exit_function(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return Error::new(
            Span::call_site(),
            "main_loop_exit_function takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_main_loop_function(fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_main_loop_function(function: ItemFn) -> Result<TokenStream2> {
    let name = LitStr::new(&function.sig.ident.to_string(), function.sig.ident.span());
    expand_registered_function(
        InitFnArgs {
            name,
            runs_before: Vec::new(),
            runs_after: Vec::new(),
        },
        function,
    )
}

/// Registers a per-worker init function in the topologically-sorted worker init chain.
///
/// Example:
/// ```ignore
/// #[worker_init_function(name = "tcp_worker_init", runs_after = ["generic_worker_init"])]
/// fn tcp_worker_init(vm: &mut Engine, tcp: Arc<TcpMain>) -> RuntimeResult<()> { ... }
/// ```
#[proc_macro_attribute]
pub fn worker_init_function(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as InitFnArgs);
    let fn_item = parse_macro_input!(input as syn::ItemFn);
    expand_registered_function(args, fn_item)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

struct PluginArgs {
    name: LitStr,
    load_after: Vec<LitStr>,
    ip_output: Option<Expr>,
    init_functions: Vec<Path>,
    config_functions: Vec<Path>,
    early_config_functions: Vec<Path>,
    main_loop_enter_functions: Vec<Path>,
    main_loop_exit_functions: Vec<Path>,
    worker_init_functions: Vec<Path>,
    graph_nodes: Vec<Path>,
    node_functions: Vec<Path>,
    process_nodes: Vec<Path>,
    session_transports: Vec<Path>,
    app_session_protocols: Vec<Path>,
    binary_api_methods: Vec<Path>,
}

impl Parse for PluginArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut load_after = Vec::new();
        let mut ip_output = None;
        let mut init_functions = Vec::new();
        let mut config_functions = Vec::new();
        let mut early_config_functions = Vec::new();
        let mut main_loop_enter_functions = Vec::new();
        let mut main_loop_exit_functions = Vec::new();
        let mut worker_init_functions = Vec::new();
        let mut graph_nodes = Vec::new();
        let mut node_functions = Vec::new();
        let mut process_nodes = Vec::new();
        let mut session_transports = Vec::new();
        let mut app_session_protocols = Vec::new();
        let mut binary_api_methods = Vec::new();
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
                "load_after" => load_after = parse_litstr_array(input)?,
                "init_functions" => init_functions = parse_path_array(input)?,
                "config_functions" => config_functions = parse_path_array(input)?,
                "early_config_functions" => early_config_functions = parse_path_array(input)?,
                "main_loop_enter_functions" => {
                    main_loop_enter_functions = parse_path_array(input)?;
                }
                "main_loop_exit_functions" => {
                    main_loop_exit_functions = parse_path_array(input)?;
                }
                "worker_init_functions" => worker_init_functions = parse_path_array(input)?,
                "graph_nodes" => graph_nodes = parse_path_array(input)?,
                "node_functions" => node_functions = parse_path_array(input)?,
                "process_nodes" => process_nodes = parse_path_array(input)?,
                "session_transports" => session_transports = parse_path_array(input)?,
                "app_session_protocols" => app_session_protocols = parse_path_array(input)?,
                "binary_api_methods" => binary_api_methods = parse_path_array(input)?,
                "ip_output" => {
                    if ip_output.is_some() {
                        return Err(Error::new(key.span(), "duplicate `ip_output` argument"));
                    }
                    ip_output = Some(input.parse()?);
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!(
                            "unknown `plugin` argument `{other}`; expected `name`, `load_after`, or `ip_output`"
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
            load_after,
            ip_output,
            init_functions,
            config_functions,
            early_config_functions,
            main_loop_enter_functions,
            main_loop_exit_functions,
            worker_init_functions,
            graph_nodes,
            node_functions,
            process_nodes,
            session_transports,
            app_session_protocols,
            binary_api_methods,
        })
    }
}

fn attribute_macro_leaf(path: &Path) -> Option<String> {
    path.segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn is_plugin_attribute(attribute: &Attribute) -> bool {
    attribute_macro_leaf(attribute.path()).is_some_and(|leaf| leaf == "plugin")
}

/// Exports the one `abi_stable` root module used by a dynamic plugin.
///
/// ```ignore
/// hammer_component_macros::declare_plugin!(name = "tun", load_after = []);
///
/// #[graph_node(...)]
/// struct TunInputDriverNode;
/// ```
#[proc_macro]
pub fn declare_plugin(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as PluginArgs);
    plugin_registration_tokens(&args).into()
}

/// Marks a module as a dynamic plugin and emits its metadata plus registration
/// image declaration.
///
/// ```ignore
/// #[plugin(name = "tun", load_after = ["device", "interface"])]
/// mod tun { ... }
/// ```
#[proc_macro_attribute]
pub fn plugin(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as PluginArgs);
    let mut module = parse_macro_input!(input as ItemMod);
    expand_plugin(args, &mut module)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn plugin_registration_tokens(args: &PluginArgs) -> TokenStream2 {
    let name = &args.name;
    let load_after = &args.load_after;
    let dependency_len = load_after.len();
    let init_functions = &args.init_functions;
    let config_functions = &args.config_functions;
    let early_config_functions = &args.early_config_functions;
    let main_loop_enter_functions = &args.main_loop_enter_functions;
    let main_loop_exit_functions = &args.main_loop_exit_functions;
    let worker_init_functions = &args.worker_init_functions;
    let graph_nodes = &args.graph_nodes;
    let node_functions = &args.node_functions;
    let process_nodes = &args.process_nodes;
    let session_transports = &args.session_transports;
    let app_session_protocols = &args.app_session_protocols;
    let binary_api_methods = &args.binary_api_methods;
    let dependencies_ident = format_ident!(
        "__PLUGIN_LOAD_AFTER_{}",
        name.value().to_ascii_uppercase().replace('-', "_")
    );
    let ip_output = match &args.ip_output {
        Some(output) => quote! {
            ::hammer_runtime::__private::ROption::RSome(
                ::hammer_runtime::__private::RRef::new(#output)
            )
        },
        None => quote!(::hammer_runtime::__private::ROption::RNone),
    };
    quote! {
        ::hammer_runtime::__declare_registration_image!(
            init_functions = [#(#init_functions),*];
            config_functions = [#(#config_functions),*];
            early_config_functions = [#(#early_config_functions),*];
            main_loop_enter_functions = [#(#main_loop_enter_functions),*];
            main_loop_exit_functions = [#(#main_loop_exit_functions),*];
            worker_init_functions = [#(#worker_init_functions),*];
            graph_nodes = [#(#graph_nodes),*];
            node_functions = [#(#node_functions),*];
            process_nodes = [#(#process_nodes),*];
            session_transports = [#(#session_transports),*];
            app_session_protocols = [#(#app_session_protocols),*];
            binary_api_methods = [#(#binary_api_methods),*];
        );

        // This is deliberately plain TOML data, not an executable entrypoint.
        // PluginMain reads it from the DSO before dlopen to resolve load_after.
        const __HAMMER_PLUGIN_MANIFEST_TOML: &str = concat!(
            "name = ", stringify!(#name), "\n",
            "version = \"", env!("CARGO_PKG_VERSION"), "\"\n",
            "version_required = \"", env!("CARGO_PKG_VERSION"), "\"\n",
            "load_after = [", #( stringify!(#load_after), ",",)* "]\n",
        );

        #[used]
        #[cfg_attr(
            any(target_os = "macos", target_os = "ios", target_os = "tvos"),
            unsafe(link_section = "__DATA,__hammer_plugin")
        )]
        #[cfg_attr(
            not(any(target_os = "macos", target_os = "ios", target_os = "tvos")),
            unsafe(link_section = ".hammer_plugin")
        )]
        static __HAMMER_PLUGIN_MANIFEST: [u8; __HAMMER_PLUGIN_MANIFEST_TOML.len()] = {
            let source = __HAMMER_PLUGIN_MANIFEST_TOML.as_bytes();
            let mut bytes = [0; __HAMMER_PLUGIN_MANIFEST_TOML.len()];
            let mut index = 0;
            while index < bytes.len() {
                bytes[index] = source[index];
                index += 1;
            }
            bytes
        };

        static #dependencies_ident: [::hammer_runtime::__private::RStr<'static>; #dependency_len] = [
            #(::hammer_runtime::__private::RStr::from_str(#load_after)),*
        ];

        #[::hammer_runtime::__private::export_root_module]
        #[doc(hidden)]
        pub fn plugin_module() -> ::hammer_runtime::PluginModuleRef {
            let metadata = ::hammer_runtime::PluginMetadata::new(
                ::hammer_runtime::__private::RStr::from_str(#name),
                ::hammer_runtime::__private::RStr::from_str(env!("CARGO_PKG_VERSION")),
                ::hammer_runtime::__private::RStr::from_str(env!("CARGO_PKG_VERSION")),
                ::hammer_runtime::__private::RSlice::from_slice(&#dependencies_ident),
            );
            <::hammer_runtime::PluginModule as ::hammer_runtime::__private::PrefixTypeTrait>::leak_into_prefix(
                ::hammer_runtime::PluginModule::new(
                    metadata,
                    ::hammer_runtime::__private::RRef::new(&__HAMMER_REGISTRATION_IMAGE),
                    #ip_output,
                )
            )
        }
    }
}

fn expand_plugin(args: PluginArgs, module: &mut ItemMod) -> Result<TokenStream2> {
    if module.attrs.iter().any(is_plugin_attribute) {
        return Err(Error::new(
            module.span(),
            "nested `#[plugin]` is not allowed",
        ));
    }
    let registration = plugin_registration_tokens(&args);
    Ok(quote! {
        #registration

        #module
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand_registered_test_function(arguments: &str, function: &str) -> String {
        let arguments = syn::parse_str::<InitFnArgs>(arguments).expect("parse init arguments");
        let function = syn::parse_str::<ItemFn>(function).expect("parse init function");
        expand_registered_function(arguments, function)
            .expect("expand init function")
            .to_string()
    }

    #[test]
    fn init_function_rejects_when_predicates() {
        let error = match syn::parse_str::<InitFnArgs>(r#"name = "tun", when = |_| true"#) {
            Ok(_) => panic!("when predicates must not be part of init registration"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("unknown argument `when`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn init_adapter_injects_required_arc_from_registry() {
        let expanded = expand_registered_test_function(
            r#"name = "device", runs_before = ["workers"]"#,
            "fn init_device(engine: &mut Engine, device: Arc<DeviceMain>) -> RuntimeResult<()> { use_device(engine, device) }",
        );

        assert!(expanded.contains("registry . require :: < DeviceMain > () ?"));
        assert!(expanded.contains("init_device (__hammer_engine , __hammer_injected_1)"));
        assert!(expanded.contains("func : __hammer_init_adapter_init_device"));
        assert!(expanded.contains("runs_before : & [\"workers\"]"));
    }

    #[test]
    fn init_adapter_skips_optional_arc_when_registry_entry_is_absent() {
        let expanded = expand_registered_test_function(
            r#"name = "tun-worker""#,
            "fn init_tun_worker(engine: &mut Engine, #[inject(optional)] tun: Arc<TunControl>) -> RuntimeResult<()> { use_tun(engine, tun) }",
        );

        assert!(expanded.contains("registry . get :: < TunControl > ()"));
        assert!(expanded.contains("let Some (__hammer_injected_1)"));
        assert!(expanded.contains("else { return Ok (()) ; }"));
        assert!(
            !expanded.contains("inject (optional)"),
            "the adapter-only parameter attribute must be removed: {expanded}"
        );
    }

    #[test]
    fn init_adapter_publishes_arc_results() {
        let expanded = expand_registered_test_function(
            r#"name = "device""#,
            "fn init_device(engine: &mut Engine) -> RuntimeResult<Arc<DeviceMain>> { make_device(engine) }",
        );

        assert!(expanded.contains("let __hammer_produced = init_device (__hammer_engine) ?"));
        assert!(expanded.contains("__hammer_engine . registry . set (__hammer_produced)"));
        assert!(expanded.contains("Ok (())"));
    }

    #[test]
    fn init_adapter_supports_provider_without_engine_parameter() {
        let expanded = expand_registered_test_function(
            r#"name = "device""#,
            "fn init_device(config: Arc<Config>) -> RuntimeResult<Arc<DeviceMain>> { make_device(config) }",
        );

        assert!(expanded.contains("registry . require :: < Config > () ?"));
        assert!(expanded.contains("init_device (__hammer_injected_0) ?"));
        assert!(expanded.contains("registry . set (__hammer_produced)"));
    }

    #[test]
    fn init_adapter_publishes_present_optional_arc_results() {
        let expanded = expand_registered_test_function(
            r#"name = "tun""#,
            "fn init_tun(engine: &mut Engine) -> RuntimeResult<Option<Arc<TunControl>>> { make_tun(engine) }",
        );

        assert!(
            expanded.contains("if let Some (__hammer_produced) = init_tun (__hammer_engine) ?")
        );
        assert!(expanded.contains("__hammer_engine . registry . set (__hammer_produced)"));
    }

    #[test]
    fn config_function_arguments_include_ordering_and_early_phase() {
        let arguments = syn::parse_str::<ConfigFnArgs>(
            r#"name = "session", section = "network", early = true, runs_before = ["tcp"], runs_after = ["device"]"#,
        )
        .expect("parse config arguments");

        assert!(arguments.early);
        assert_eq!(arguments.init.name.value(), "session");
        assert_eq!(arguments.section.value(), "network");
        assert_eq!(arguments.init.runs_before[0].value(), "tcp");
        assert_eq!(arguments.init.runs_after[0].value(), "device");
    }

    #[test]
    fn config_function_uses_the_same_init_adapter_registration() {
        let arguments = syn::parse_str::<ConfigFnArgs>(
            r#"name = "session", section = "network", runs_after = ["transport"]"#,
        )
        .expect("parse config arguments");
        let function = syn::parse_str::<ItemFn>(
            "fn configure_session(config: SessionConfig, engine: &mut Engine) -> RuntimeResult<Option<Arc<Session>>> { configure(config, engine) }",
        )
        .expect("parse config function");
        let expanded = expand_config_function(arguments, function)
            .expect("expand config function")
            .to_string();

        assert!(expanded.contains("static __CONFIG_FN_SESSION"));
        assert!(expanded.contains("init :: ConfigFunction"));
        assert!(expanded.contains("ConfigFunction"));
        assert!(expanded.contains("func : __hammer_config_adapter_configure_session"));
        assert!(expanded.contains("runs_after : & [\"transport\"]"));
    }

    #[test]
    fn main_loop_function_uses_the_same_init_adapter() {
        let function = syn::parse_str::<ItemFn>(
            "fn start_workers(engine: &mut Engine, handoff: Arc<HandoffMain>) -> RuntimeResult<()> { start(engine, handoff) }",
        )
        .expect("parse main-loop function");
        let expanded = expand_main_loop_function(function)
            .expect("expand main-loop function")
            .to_string();

        assert!(expanded.contains("static __INIT_FN_START_WORKERS"));
        assert!(expanded.contains("registry . require :: < HandoffMain > () ?"));
        assert!(expanded.contains("func : __hammer_init_adapter_start_workers"));
    }

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

        assert!(expanded.contains("Simd :: < u8 , 1usize > :: splat (0)"));
        assert!(expanded.contains("SIMD128"));
        assert!(expanded.contains("target_feature (enable = \"avx2\")"));
        assert!(expanded.contains("target_feature (enable = \"neon\")"));
        assert!(expanded.contains("NodeFunctionRegistration :: new"));
        assert!(expanded.matches("device-tx").count() > 1);
        assert!(!expanded.contains("NodeEntry"));
    }

    #[test]
    fn node_function_passes_simd_width_to_const_generic_body() {
        let args = syn::parse_str::<NodeFunctionArgs>("node = DeviceTxNode")
            .expect("parse Node Function args");
        let function = syn::parse_str::<ItemFn>(
            "fn device_tx<const SIMD_BYTES: usize>(runtime: &Runtime, data: Data, frame: &mut Frame) -> Result { body::<SIMD_BYTES>() }",
        )
        .expect("parse generic Node Function");
        let expanded = expand_node_function(args, function).to_string();

        assert!(expanded.contains("device_tx :: < 1usize >"));
        assert!(expanded.contains("__device_tx_simd128 :: < 16usize >"));
        assert!(expanded.contains("__device_tx_simd256 :: < 32usize >"));
        assert!(expanded.contains("__device_tx_simd512 :: < 64usize >"));
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
        let expanded = expand_node(args, item, None, true, true)
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
        let expanded = expand_node(args, item, None, true, true)
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
            expanded.contains("static __SERVICE_GRAPH_NODE_INPUT_SIBLING_NODE"),
            "missing explicit graph registration: {expanded}"
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
            !expanded.contains("NodeId :: new (0)"),
            "named-next graph init must not construct placeholder node ids: {expanded}"
        );
        assert!(
            !expanded.contains("next : ["),
            "named-next graph node must not store resolved next ids: {expanded}"
        );
        assert!(
            expanded.contains("NodeState :: Disabled"),
            "missing generated disabled state: {expanded}"
        );
        assert!(
            expanded.contains("init : __service_graph_node_input_owner_node_init"),
            "static entry must use generated init: {expanded}"
        );
        assert!(
            expanded.contains("static __SERVICE_GRAPH_NODE_INPUT_OWNER_NODE"),
            "missing explicit graph registration: {expanded}"
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

    #[test]
    fn plugin_on_extern_mod_emits_registration_without_body_injection() {
        let args = syn::parse_str::<PluginArgs>(r#"name = "tun", load_after = ["device"]"#)
            .expect("parse plugin args");
        let mut module = syn::parse_str::<ItemMod>("mod tun;").expect("parse extern mod");
        let expanded = expand_plugin(args, &mut module)
            .expect("extern #[plugin] emits registration")
            .to_string();
        assert!(expanded.contains("export_root_module"));
        assert!(expanded.contains("PluginModule"));
        assert!(expanded.contains("mod tun ;"));
    }

    #[test]
    fn declare_plugin_tokens_emit_one_root_module() {
        let args = syn::parse_str::<PluginArgs>(r#"name = "device", load_after = []"#)
            .expect("parse plugin args");
        let expanded = plugin_registration_tokens(&args).to_string();
        assert!(expanded.contains("export_root_module"));
        assert!(expanded.contains("PluginMetadata"));
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
