//! Rust declarations for Binary API definitions. No sibling-source discovery or
//! process-global macro inventory: referenced definitions are Rust constants.

use heck::{ToShoutySnakeCase, ToSnakeCase};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Fields, Ident, Lit, LitStr, Path, Result, Type,
    spanned::Spanned,
};

#[derive(Default)]
struct Attributes {
    name: Option<LitStr>,
    returns: Option<Path>,
    stream: Option<Path>,
    events: Option<Vec<Path>>,
    autoreply: Option<Ident>,
    string: bool,
    legacy: bool,
    alias: bool,
    enumflag: bool,
    backwards_compatible: bool,
    length: Option<LitStr>,
    flags: Vec<Ident>,
    options: Vec<(Ident, Option<LitStr>)>,
}

fn attributes(attrs: &[Attribute]) -> Result<Attributes> {
    let mut result = Attributes::default();
    let mut seen = std::collections::HashSet::new();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("api")) {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .ok_or_else(|| meta.error("expected an API attribute"))?;
            let spelling = key.to_string();
            if spelling != "option" && !seen.insert(spelling.clone()) {
                return Err(meta.error("duplicate API attribute"));
            }
            match spelling.as_str() {
                "name" => result.name = Some(meta.value()?.parse()?),
                "returns" => result.returns = Some(meta.value()?.parse()?),
                "stream" => result.stream = Some(meta.value()?.parse()?),
                "autoreply" => result.autoreply = Some(meta.value()?.parse()?),
                "length" => result.length = Some(meta.value()?.parse()?),
                "string" => result.string = true,
                "legacy" => result.legacy = true,
                "alias" => result.alias = true,
                "enumflag" => result.enumflag = true,
                "backwards_compatible" => result.backwards_compatible = true,
                "dont_trace" | "manual_print" | "manual_endian" | "autoendian" => {
                    result.flags.push(key.clone());
                }
                "events" => {
                    let content;
                    syn::parenthesized!(content in meta.input);
                    let paths = content.parse_terminated(Path::parse_mod_style, syn::Token![,])?;
                    if paths.is_empty() {
                        return Err(meta.error("events requires at least one message type"));
                    }
                    result.events = Some(paths.into_iter().collect());
                }
                "option" => {
                    meta.parse_nested_meta(|option| {
                        let key = option
                            .path
                            .get_ident()
                            .ok_or_else(|| option.error("expected an option name"))?;
                        if !matches!(
                            key.to_string().as_str(),
                            "version" | "deprecated" | "status" | "vat_help"
                        ) {
                            return Err(option.error("unsupported API option"));
                        }
                        if result.options.iter().any(|(name, _)| name == key) {
                            return Err(option.error("duplicate API option"));
                        }
                        let value = if option.input.peek(syn::Token![=]) {
                            Some(option.value()?.parse()?)
                        } else {
                            None
                        };
                        result.options.push((key.clone(), value));
                        Ok(())
                    })?;
                }
                _ => return Err(meta.error("unknown API attribute")),
            }
            Ok(())
        })?;
    }
    Ok(result)
}

fn reject_serde_layout(attrs: &[Attribute]) -> Result<()> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                let _: LitStr = meta.value()?.parse()?;
                Ok(())
            } else {
                Err(meta.error("Serde layout attributes are not supported by this API definition; supply a matching manual Serde implementation"))
            }
        })?;
    }
    Ok(())
}

fn protocol_name(value: &str, span: Span) -> Result<LitStr> {
    if value.is_empty()
        || !value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index != 0 && byte.is_ascii_digit())
        })
    {
        return Err(Error::new(
            span,
            "API names must be ASCII protocol identifiers",
        ));
    }
    Ok(LitStr::new(value, span))
}

fn owner() -> Result<TokenStream> {
    let name = proc_macro_crate::crate_name("hammer-ipc")
        .map_err(|error| Error::new(Span::call_site(), error))?;
    let name = match name {
        proc_macro_crate::FoundCrate::Itself => "hammer_ipc".to_owned(),
        proc_macro_crate::FoundCrate::Name(name) => name,
    };
    let name = Ident::new(&name, Span::call_site());
    Ok(quote!(::#name::binary_api::definition))
}

fn primitive(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else { return None };
    if path.qself.is_some()
        || path
            .path
            .segments
            .iter()
            .any(|s| !matches!(s.arguments, syn::PathArguments::None))
    {
        return None;
    }
    let names: Vec<_> = path
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();
    let name = names.last()?;
    if !(names.len() == 1
        || (names.len() == 3
            && matches!(names[0].as_str(), "std" | "core")
            && names[1] == "primitive"))
    {
        return None;
    }
    matches!(
        name.as_str(),
        "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "bool" | "f64"
    )
    .then(|| name.clone())
}

fn field_type(ty: &Type, owner: &TokenStream) -> Result<(TokenStream, TokenStream)> {
    if let Some(name) = primitive(ty) {
        return Ok((quote!(#name), quote!(None)));
    }
    match ty {
        Type::Path(path)
            if path.qself.is_none()
                && path
                    .path
                    .segments
                    .iter()
                    .all(|s| matches!(s.arguments, syn::PathArguments::None)) =>
        {
            Ok((
                quote!(<#ty as #owner::Typedef>::NAME),
                quote!(Some(<#ty as #owner::Typedef>::BLOCK)),
            ))
        }
        _ => Err(Error::new_spanned(
            ty,
            "API fields require a protocol primitive or concrete Typedef",
        )),
    }
}

fn vector_element(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != "Vec" || path.qself.is_some() {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match args.args.first()? {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    }
}

fn field(field: &syn::Field, owner: &TokenStream, fallback: &str) -> Result<(LitStr, TokenStream)> {
    reject_serde_layout(&field.attrs)?;
    let attrs = attributes(&field.attrs)?;
    if attrs.returns.is_some()
        || attrs.stream.is_some()
        || attrs.events.is_some()
        || attrs.autoreply.is_some()
        || attrs.alias
        || attrs.enumflag
        || attrs.backwards_compatible
        || !attrs.flags.is_empty()
        || !attrs.options.is_empty()
    {
        return Err(Error::new_spanned(
            field,
            "only name and length are API field attributes",
        ));
    }
    let rust_name = field
        .ident
        .as_ref()
        .map(|id| id.to_string().trim_start_matches("r#").to_owned());
    let name = match attrs.name {
        Some(name) => protocol_name(&name.value(), name.span())?,
        None => protocol_name(rust_name.as_deref().unwrap_or(fallback), field.span())?,
    };
    if attrs.string {
        if attrs.length.is_some() || attrs.legacy {
            return Err(Error::new_spanned(
                field,
                "API string has its own length representation",
            ));
        }
        let length = match &field.ty {
            Type::Array(array) if primitive(&array.elem).as_deref() == Some("u8") => {
                let length = &array.len;
                quote!(#length)
            }
            Type::Path(_) => quote!(0),
            _ => {
                return Err(Error::new_spanned(
                    field,
                    "API string requires bytes or an owned API String",
                ));
            }
        };
        return Ok((
            name.clone(),
            quote!(#owner::Field {
                name: #name, field_type: "string", block: None, length: Some(#length), length_field: None,
            }),
        ));
    }
    if attrs.legacy && vector_element(&field.ty).is_none() {
        return Err(Error::new_spanned(
            field,
            "legacy tail arrays require a Vec and explicit Serde",
        ));
    }
    let (ty, length, length_field) = if let Type::Array(array) = &field.ty {
        if attrs.length.is_some() {
            return Err(Error::new_spanned(
                field,
                "a fixed API array cannot also have a length field",
            ));
        }
        let length = &array.len;
        (array.elem.as_ref(), quote!(Some(#length)), quote!(None))
    } else if attrs.legacy {
        if attrs.length.is_some() {
            return Err(Error::new_spanned(
                field,
                "legacy arrays have no count field",
            ));
        }
        let element = vector_element(&field.ty).expect("legacy array shape checked above");
        (element, quote!(Some(0)), quote!(None))
    } else if let Some(element) = vector_element(&field.ty) {
        let length = attrs.length.ok_or_else(|| Error::new_spanned(field, "a counted API Vec requires length = \"preceding_field\" and a matching manual Serde implementation"))?;
        protocol_name(&length.value(), length.span())?;
        (element, quote!(Some(0)), quote!(Some(#length)))
    } else {
        if attrs.length.is_some() {
            return Err(Error::new_spanned(
                field,
                "length applies only to a counted API Vec",
            ));
        }
        (&field.ty, quote!(None), quote!(None))
    };
    let (type_name, block) = field_type(ty, owner)?;
    Ok((
        name.clone(),
        quote!(#owner::Field {
            name: #name,
            field_type: #type_name,
            block: #block,
            length: #length,
            length_field: #length_field,
        }),
    ))
}

fn fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
    owner: &TokenStream,
    message: bool,
) -> Result<Vec<TokenStream>> {
    let mut output = Vec::new();
    let mut names = std::collections::HashSet::new();
    for (index, declaration) in fields.iter().enumerate() {
        if message && index == 0 {
            if declaration.ident.as_ref().is_none_or(|name| name != "id")
                || primitive(&declaration.ty).as_deref() != Some("u16")
            {
                return Err(Error::new_spanned(
                    declaration,
                    "API messages require id: u16 as their first field",
                ));
            }
            if declaration
                .attrs
                .iter()
                .any(|attr| attr.path().is_ident("api") || attr.path().is_ident("serde"))
            {
                return Err(Error::new_spanned(
                    declaration,
                    "the message id field cannot change its protocol layout",
                ));
            }
            continue;
        }
        let attrs = attributes(&declaration.attrs)?;
        if let Some(length) = attrs.length
            && !names.contains(&length.value())
        {
            return Err(Error::new(
                length.span(),
                "API length must reference a preceding field by its protocol name",
            ));
        }
        let (name, declaration) = field(declaration, owner, "value")?;
        if !names.insert(name.value()) {
            return Err(Error::new(name.span(), "duplicate API field name"));
        }
        output.push(declaration);
    }
    if message && fields.is_empty() {
        return Err(Error::new(
            fields.span(),
            "API messages require id: u16 as their first field",
        ));
    }
    Ok(output)
}

fn integer(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Lit(value) => match &value.lit {
            Lit::Int(value) => value.base10_parse(),
            _ => Err(Error::new_spanned(
                expr,
                "API enum values must be integer literals",
            )),
        },
        Expr::Unary(value) if matches!(value.op, syn::UnOp::Neg(_)) => integer(&value.expr)?
            .checked_neg()
            .ok_or_else(|| Error::new_spanned(expr, "API enum value exceeds i64")),
        _ => Err(Error::new_spanned(
            expr,
            "API enum values must be integer literals",
        )),
    }
}

fn enum_block(
    input: &DeriveInput,
    data: &syn::DataEnum,
    owner: &TokenStream,
    flags: bool,
) -> Result<TokenStream> {
    let mut repr = None;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("repr"))
    {
        attr.parse_nested_meta(|meta| {
            if repr.is_some() {
                return Err(meta.error("API enums require one integer repr"));
            }
            let name = meta
                .path
                .get_ident()
                .ok_or_else(|| meta.error("expected integer repr"))?
                .to_string();
            let range = match name.as_str() {
                "u8" => (0, u8::MAX as i64),
                "u16" => (0, u16::MAX as i64),
                "u32" => (0, u32::MAX as i64),
                "i8" if !flags => (i8::MIN as i64, i8::MAX as i64),
                "i16" if !flags => (i16::MIN as i64, i16::MAX as i64),
                "i32" if !flags => (i32::MIN as i64, i32::MAX as i64),
                _ => return Err(meta.error("unsupported API enum integer repr")),
            };
            repr = Some(range);
            Ok(())
        })?;
    }
    let (minimum, maximum) = repr.ok_or_else(|| Error::new_spanned(input, "API enums require an explicit supported integer repr and a numeric Serde implementation"))?;
    let mut count = -1_i64;
    let mut compatible = false;
    let mut names = std::collections::HashSet::new();
    let mut values = Vec::new();
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new_spanned(
                variant,
                "API enum variants cannot carry payloads",
            ));
        }
        reject_serde_layout(&variant.attrs)?;
        let attrs = attributes(&variant.attrs)?;
        if attrs.returns.is_some()
            || attrs.stream.is_some()
            || attrs.events.is_some()
            || attrs.autoreply.is_some()
            || attrs.string
            || attrs.legacy
            || attrs.alias
            || attrs.enumflag
            || attrs.length.is_some()
            || !attrs.flags.is_empty()
            || !attrs.options.is_empty()
        {
            return Err(Error::new_spanned(
                variant,
                "only name and backwards_compatible apply to API enum values",
            ));
        }
        count = match &variant.discriminant {
            Some((_, expr)) => integer(expr)?,
            None => count
                .checked_add(1)
                .ok_or_else(|| Error::new_spanned(variant, "API enum value overflow"))?,
        };
        if count < minimum || count > maximum {
            return Err(Error::new_spanned(
                variant,
                "API enum value does not fit its repr",
            ));
        }
        if flags && count.count_ones() > 1 {
            return Err(Error::new_spanned(
                variant,
                "API enumflag values may contain at most one set bit",
            ));
        }
        let name = match attrs.name {
            Some(name) => protocol_name(&name.value(), name.span())?,
            None => protocol_name(
                &variant.ident.to_string().to_shouty_snake_case(),
                variant.ident.span(),
            )?,
        };
        if !names.insert(name.value()) {
            return Err(Error::new(name.span(), "duplicate API enum name"));
        }
        if attrs.backwards_compatible {
            compatible = true;
        } else if compatible {
            return Err(Error::new_spanned(
                variant,
                "backwards-compatible API enum values must be last",
            ));
        } else {
            values.push(quote!((#name, #count)));
        }
    }
    Ok(quote!(#owner::Block::Enum(&[#(#values),*])))
}

pub fn derive(tokens: TokenStream, message: bool) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(tokens)?;
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &input.generics,
            "API definitions must be concrete, not generic",
        ));
    }
    reject_serde_layout(&input.attrs)?;
    let attrs = attributes(&input.attrs)?;
    let owner = owner()?;
    let ident = &input.ident;
    let visibility = &input.vis;
    let name = match &attrs.name {
        Some(name) => protocol_name(&name.value(), name.span())?,
        None => protocol_name(
            &ident.to_string().trim_start_matches("r#").to_snake_case(),
            ident.span(),
        )?,
    };
    if attrs.length.is_some() || attrs.backwards_compatible || attrs.string || attrs.legacy {
        return Err(Error::new_spanned(
            &input,
            "length and backwards_compatible are field/enum value attributes",
        ));
    }
    if !message
        && (attrs.returns.is_some()
            || attrs.stream.is_some()
            || attrs.events.is_some()
            || attrs.autoreply.is_some()
            || !attrs.options.is_empty()
            || !attrs.flags.is_empty())
    {
        return Err(Error::new_spanned(
            &input,
            "service, flags and options belong to Api messages",
        ));
    }
    if message && (attrs.alias || attrs.enumflag) {
        return Err(Error::new_spanned(
            &input,
            "alias and enumflag belong to Typedef",
        ));
    }
    if attrs.enumflag && !matches!(input.data, Data::Enum(_)) {
        return Err(Error::new_spanned(&input, "enumflag requires an enum"));
    }
    let mut alias_check = TokenStream::new();
    let block = match &input.data {
        Data::Struct(data) if attrs.alias => {
            let Fields::Unnamed(fields) = &data.fields else {
                return Err(Error::new_spanned(
                    &input,
                    "API alias requires a single-field tuple struct",
                ));
            };
            if fields.unnamed.len() != 1 {
                return Err(Error::new_spanned(
                    &input,
                    "API alias requires a single-field tuple struct",
                ));
            }
            let declaration = &fields.unnamed[0];
            if vector_element(&declaration.ty).is_some() {
                return Err(Error::new_spanned(
                    declaration,
                    "variable-length API aliases are not supported",
                ));
            }
            let (_, declaration) = field(declaration, &owner, "value")?;
            alias_check = quote!(const _: () = {
                let field: #owner::Field = #declaration;
                assert!(field.length_field.is_none() && !matches!(field.length, Some(0)), "API aliases require a nonzero fixed length");
                if let Some(block) = field.block {
                    assert!(!block.is_vla(), "API aliases require fixed-size fields");
                }
            };);
            quote!(#owner::Block::Alias)
        }
        Data::Struct(data) => {
            if attrs.enumflag {
                return Err(Error::new_spanned(&input, "enumflag requires an enum"));
            }
            let Fields::Named(named) = &data.fields else {
                return Err(Error::new_spanned(
                    &input,
                    "API definitions require named fields, or an explicit alias",
                ));
            };
            let fields = fields(&named.named, &owner, message)?;
            quote!(#owner::Block::Fields(&[#(#fields),*]))
        }
        Data::Enum(data) if !message && !attrs.alias => {
            enum_block(&input, data, &owner, attrs.enumflag)?
        }
        Data::Union(data) if !message && !attrs.alias && !attrs.enumflag => {
            let fields = fields(&data.fields.named, &owner, false)?;
            quote!(#owner::Block::Fields(&[#(#fields),*]))
        }
        _ => {
            return Err(Error::new_spanned(
                &input,
                "Api requires a named struct; Typedef supports structs, aliases, numeric enums and unions",
            ));
        }
    };
    if !message {
        let union_check = if matches!(input.data, Data::Union(_)) {
            quote!(
                const _: () = assert!(!<#ident as #owner::Typedef>::BLOCK.is_vla(), "variable-length API unions require an explicit wire implementation");
            )
        } else {
            TokenStream::new()
        };
        return Ok(quote! {
            #union_check
            impl #owner::Typedef for #ident {
                const NAME: &'static str = #name;
                const BLOCK: #owner::Block = #block;
            }
            const _: () = <#ident as #owner::Typedef>::BLOCK.validate();
            #alias_check
        });
    }
    if attrs.returns.is_some() && attrs.autoreply.is_some() {
        return Err(Error::new_spanned(
            &input,
            "returns and autoreply both declare the reply; choose one",
        ));
    }
    let null = attrs
        .returns
        .as_ref()
        .is_some_and(|path| path.is_ident("null"));
    if null && (attrs.stream.is_some() || attrs.events.is_some()) {
        return Err(Error::new_spanned(
            &input,
            "returns = null cannot declare stream or events",
        ));
    }
    if attrs.events.is_some()
        && (attrs.stream.is_some() || (attrs.returns.is_none() && attrs.autoreply.is_none()))
    {
        return Err(Error::new_spanned(
            &input,
            "events requires a reply and cannot combine with stream",
        ));
    }
    let option_values: Vec<_> = attrs
        .options
        .iter()
        .map(|(key, value)| {
            let key = key.to_string();
            let value = match value {
                Some(value) => quote!(Some(#value)),
                None => quote!(None),
            };
            quote!((#key, #value))
        })
        .collect();
    let mut flags: Vec<_> = attrs.flags.iter().map(Ident::to_string).collect();
    if attrs.autoreply.is_some() {
        flags.push("autoreply".to_owned());
    }
    let mut generated_reply = TokenStream::new();
    let reply = if let Some(reply) = &attrs.autoreply {
        let reply_name = format!("{}_reply", name.value());
        let serde_path = LitStr::new(
            &quote!(#owner::serde).to_string().replace(' ', ""),
            Span::call_site(),
        );
        let options: Vec<_> = attrs
            .options
            .iter()
            .map(|(key, value)| match value {
                Some(value) => quote!(#[api(option(#key = #value))]),
                None => quote!(#[api(option(#key))]),
            })
            .collect();
        generated_reply = quote! {
            #[derive(#owner::serde::Serialize, #owner::serde::Deserialize, #owner::Api)]
            #[serde(crate = #serde_path)]
            #[api(name = #reply_name)]
            #(#options)*
            #visibility struct #reply {
                pub id: u16,
                pub context: u32,
                pub retval: i32,
            }
        };
        Some(quote!(#reply))
    } else if null {
        None
    } else {
        attrs.returns.as_ref().map(|reply| quote!(#reply))
    };
    let stream = attrs.stream.as_ref();
    let reply = reply.or_else(|| stream.map(|details| quote!(#details)));
    let reply_name = match &reply {
        Some(reply) => quote!(Some(<#reply as #owner::Api>::NAME)),
        None => quote!(None),
    };
    let stream_message = if attrs.returns.is_some() || attrs.autoreply.is_some() {
        match stream {
            Some(stream) => quote!(Some(<#stream as #owner::Api>::NAME)),
            None => quote!(None),
        }
    } else {
        quote!(None)
    };
    let events = attrs.events.as_deref().unwrap_or(&[]);
    let is_stream = stream.is_some();
    let service = if attrs.returns.is_some() || attrs.autoreply.is_some() || is_stream {
        quote!(Some(#owner::Service {
            caller: Self::NAME, reply: #reply_name, stream: #is_stream,
            stream_message: #stream_message, events: &[#(<#events as #owner::Api>::NAME),*],
        }))
    } else {
        quote!(None)
    };
    let identity_check = match reply {
        Some(reply) => quote! {
            const _: () = assert!(
                !#owner::same_name(<#ident as #owner::Api>::NAME, <#reply as #owner::Api>::NAME),
                "API request and reply must have different protocol names",
            );
        },
        None => TokenStream::new(),
    };
    let key_length = name.value().len() + 9;
    Ok(quote! {
        impl #owner::Api for #ident {
            const NAME: &'static str = #name;
            const BLOCK: #owner::Block = #block;
            const CRC: u32 = (#block).crc();
            const NAME_CRC: &'static str = {
                const BYTES: [u8; #key_length] = #owner::name_crc::<#key_length>(
                    <#ident as #owner::Api>::NAME, <#ident as #owner::Api>::CRC,
                );
                match ::core::str::from_utf8(&BYTES) {
                    Ok(name) => name,
                    Err(_) => panic!("generated API identity must be ASCII"),
                }
            };
            const SERVICE: Option<#owner::Service> = #service;
            const OPTIONS: &'static [(&'static str, Option<&'static str>)] = &[#(#option_values),*];
            const FLAGS: &'static [&'static str] = &[#(#flags),*];
        }
        const _: () = (#block).validate();
        #identity_check
        #generated_reply
    })
}
