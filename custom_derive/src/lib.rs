extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;
use syn::DeriveInput;
use syn::Ident;

#[proc_macro_derive(Actor, attributes(message_type))]
pub fn derive_actor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // get the name of the type we want to implement the trait for
    let name = &input.ident;
    eprintln!("ident name: {}", name);
    eprintln!("attr count: {}", input.attrs.len());
    let message_type_name = input.attrs[0]
        .parse_args::<Ident>()
        .expect("Cannot parse arguments of 'message_type' attribute");

    eprintln!("first attr name: {}", &message_type_name);

    let expanded = quote! {
      impl crate::Actor for #name {
        type Message = #message_type_name;

        fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
            unimplemented!()
        }

        fn run(&mut self) {
            unimplemented!()
        }

        fn dispatch_message(&mut self, message: Self::Message) -> ShouldTerminate {
            unimplemented!()
        }
      }
    };

    TokenStream::from(expanded)
}
