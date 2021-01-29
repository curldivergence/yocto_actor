extern crate proc_macro;
extern crate proc_macro2;

use heck::SnakeCase;
use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Actor, attributes(worker))]
pub fn derive_actor(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    // get the name of the type we want to implement the trait for
    let enum_name = &input.ident;
    // eprintln!("ident name: {}", enum_name);

    // eprintln!("attr count: {}", input.attrs.len());
    let worker_type_name = input.attrs[0]
        .parse_args::<Ident>()
        .expect("Cannot parse arguments of 'message_type' attribute");

    // eprintln!("first attr name: {}", &worker_type_name);
    let enum_data = if let syn::Data::Enum(data) = &input.data {
        data
    } else {
        panic!("{} is not an enum", enum_name);
    };

    let mut dispatch_arms = TokenStream::new();

    for variant_data in &enum_data.variants {
        let variant_name = &variant_data.ident;
        let handler_method_name = Ident::new(
            &format!("handle_{}", &variant_name).to_snake_case(),
            Span::call_site(),
        );
        // eprintln!(
        //     "Found variant {}, handler function name: {}",
        //     &variant_name, handler_method_name
        // );

        let variant_type = match &variant_data.fields {
            syn::Fields::Unit => {
                // eprintln!("variant type: unit");
                let current_arm = quote! (
                    Self::Message::#variant_name => self. #handler_method_name(),
                );
                // eprintln!("Current arm: {}", &current_arm);
                dispatch_arms.extend(current_arm);
            }
            syn::Fields::Unnamed(unnamed) => {
                // eprintln!("variant type: unnamed");
                unimplemented!("Tuple variants are not supported") // ToDo
            }
            syn::Fields::Named(named_fields) => {
                // eprintln!("variant type: named");

                // let mut handler_arguments = TokenStream::new();
                let mut destructured_fields = TokenStream::new();

                for field in named_fields.named.iter() {
                    let field_name = &field.ident.as_ref().expect("expected a named field");
                    let field_type = &field.ty;
                    // eprintln!(
                    //     "Found named field: name {}, type {}",
                    //     field_name,
                    //     field_type.to_token_stream().to_string()
                    // );
                    destructured_fields.extend(quote!(#field_name,));
                    // handler_arguments.extend(quote! (#field_name : #field_type,));
                }

                let current_arm = quote! (
                    Self::Message::#variant_name{ #destructured_fields } => self. #handler_method_name(#destructured_fields),
                );
                // eprintln!("Current arm: {}", &current_arm);
                dispatch_arms.extend(current_arm);
            }
        };
    }

    let expanded = quote! {
      impl Actor for #worker_type_name {
        type Message = #enum_name;

        fn run(&mut self) {
            loop {
                self.pre_run();

                let message_bytes = self.inbox.receive();
                let message: Self::Message =
                    bincode::deserialize(&message_bytes).expect("Actor cannot deserialize message");
                if self.dispatch_message(message).0 {
                    break;
                }

                self.post_run();
            }
        }

        fn dispatch_message(&mut self, message: Self::Message) -> ShouldTerminate {
            match message {
                #dispatch_arms
            }
        }
      }
    };

    proc_macro::TokenStream::from(expanded)
}
