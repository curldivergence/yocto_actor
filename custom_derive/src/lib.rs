extern crate proc_macro;
extern crate proc_macro2;

use heck::SnakeCase;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_attribute]
pub fn actor_message(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    // get the name of the type we want to implement the trait for
    let enum_name = &input.ident;
    // eprintln!("[yocto_actor][actor_message] enum name: {}", enum_name);

    let mut expanded = TokenStream::new();

    let enum_data = if let syn::Data::Enum(data) = &input.data {
        data
    } else {
        panic!("[yocto_actor][actor_message] {} is not an enum", enum_name);
    };

    let trait_name = Ident::new(&format!("{}Handler", &enum_name), Span::call_site());

    // eprintln!("[yocto_actor][actor_message] trait name: {}", trait_name);

    let mut dispatch_arms = TokenStream::new();
    let mut handler_prototypes = TokenStream::new();

    for variant_data in &enum_data.variants {
        let variant_name = &variant_data.ident;
        let handler_method_name = Ident::new(
            &format!("handle_{}", &variant_name).to_snake_case(),
            Span::call_site(),
        );

        // eprintln!(
        //     "[yocto_actor][actor_message] found variant {}, handler function name: {}",
        //     &variant_name, handler_method_name
        // );

        match &variant_data.fields {
            syn::Fields::Unit => {
                // eprintln!("[yocto_actor][actor_message] variant type: unit");
                let current_arm = quote! (
                    #enum_name::#variant_name => self. #handler_method_name(),
                );
                // eprintln!("[yocto_actor][actor_message] Current arm: {}", &current_arm);
                dispatch_arms.extend(current_arm);
                handler_prototypes.extend(quote! {
                    fn #handler_method_name(&mut self) -> ShouldTerminate;
                });
            }
            syn::Fields::Unnamed(_unnamed) => {
                // eprintln!("[yocto_actor][actor_message] variant type: unnamed");
                unimplemented!("Tuple variants are not supported") // ToDo
            }
            syn::Fields::Named(named_fields) => {
                // eprintln!("[yocto_actor][actor_message] variant type: named");

                let mut handler_arguments = TokenStream::new();
                let mut destructured_fields = TokenStream::new();

                for field in named_fields.named.iter() {
                    let field_name = &field.ident.as_ref().expect("expected a named field");
                    let field_type = &field.ty;
                    // eprintln!(
                    //     "[yocto_actor][actor_message] Found named field: name {}, type {}",
                    //     field_name,
                    //     field_type.to_token_stream().to_string()
                    // );
                    destructured_fields.extend(quote!(#field_name,));
                    handler_arguments.extend(quote! (#field_name : #field_type,));
                }

                let current_arm = quote! (
                    #enum_name::#variant_name{ #destructured_fields } => self. #handler_method_name(#destructured_fields),
                );
                // eprintln!("[yocto_actor][actor_message] Current arm: {}", &current_arm);
                dispatch_arms.extend(current_arm);

                handler_prototypes.extend(quote! {
                    fn #handler_method_name(&mut self, #handler_arguments) -> ShouldTerminate;
                });
            }
        };
    }

    expanded.extend(quote! {
        #input

        pub trait #trait_name {
            fn pre_run(&mut self) {}
            fn post_run(&mut self) {}

            fn receive(&self) -> #enum_name;

            fn run(&mut self) {
                loop {
                    self.pre_run();

                    let message = self.receive();
                    if self.dispatch_message(message).into() {
                        break;
                    }

                    self.post_run();
                }
            }

            fn dispatch_message(&mut self, message: #enum_name) -> ShouldTerminate {
                match message {
                    #dispatch_arms
                }
            }

            #handler_prototypes
        }
    });
    // eprintln!("[yocto_actor][actor_message] final result: {}", expanded);
    proc_macro::TokenStream::from(expanded)
}
