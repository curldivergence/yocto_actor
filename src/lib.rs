use bincode::config;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Address {
    pub conn_string: String,
}

impl From<&str> for Address {
    fn from(conn_string: &str) -> Self {
        Self {
            conn_string: conn_string.to_owned(),
        }
    }
}

pub trait Actor {
    type Message;

    fn run(&mut self);
    fn pre_run(&mut self) {}
    fn post_run(&mut self) {}
    fn dispatch_message(&mut self, message: Self::Message) -> ShouldTerminate;
}

// ToDo: Result?
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ShouldTerminate(bool);

impl From<bool> for ShouldTerminate {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

impl Into<bool> for ShouldTerminate {
    fn into(self) -> bool {
        self.0
    }
}

pub use custom_derive::Actor;

// ToDo: store receiver name or some kind of id?
pub struct Outbox {
    control_socket: zmq::Socket,
}

impl Outbox {
    pub fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
        let control_socket = zmq_ctx
            .socket(zmq::PUSH)
            .expect("Cannot create control socket");
        control_socket
            .connect(&address.conn_string)
            .expect("Cannot connect control socket");

        Self { control_socket }
    }

    pub fn send<MessageType: serde::Serialize>(&self, message: &MessageType) {
        let message_bytes = bincode::serialize(message).expect("Cannot serialize message");
        self.control_socket
            .send(&message_bytes, 0)
            .expect("Cannot send message to worker");
    }
}

pub struct Inbox {
    control_socket: zmq::Socket,
}

// ToDo: yeah, this duplication is sad, but will do for now
impl Inbox {
    pub fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
        let control_socket = zmq_ctx
            .socket(zmq::PULL)
            .expect("Cannot create control socket");
        control_socket
            .bind(&address.conn_string)
            .expect("Cannot connect control socket");

        Inbox { control_socket }
    }

    pub fn receive(&self) -> Vec<u8> {
        self.control_socket
            .recv_bytes(0)
            .expect("Actor cannot receive message bytes")
    }
}

#[cfg(test)]
mod tests {
    use crate::{Actor, Address, Inbox, Outbox, ShouldTerminate};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    enum FirstMessageType {
        MessageA,
        MessageB(u8, String),
        MessageC { c_foo: u64, c_bar: String },
    }

    trait FirstMessageTypeTrait {
        fn run(&mut self);
        fn pre_run(&mut self) {}
        fn post_run(&mut self) {}
        fn dispatch_message(&mut self, message: FirstMessageType) -> ShouldTerminate {
            match message {
                FirstMessageType::MessageA => self.handle_message_a(),
                FirstMessageType::MessageB(val1, val2) => self.handle_message_b((val1, val2)),
                FirstMessageType::MessageC { c_foo, c_bar } => self.handle_message_c(c_foo, c_bar),
            }
        }

        // Event handlers

        fn handle_message_a(&mut self) -> ShouldTerminate;
        fn handle_message_b(&mut self, data: (u8,  String)) -> ShouldTerminate;
        fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate;
    }

    struct TestWorker1 {
        inbox: Inbox,
        payload: u64,
    }

    impl FirstMessageTypeTrait for TestWorker1 {
        fn run(&mut self) {
            loop {
                let message_bytes = self.inbox.receive();
                let message: FirstMessageType =
                    bincode::deserialize(&message_bytes).expect("Actor cannot deserialize message");
                if self.dispatch_message(message).0 {
                    break;
                }
            }
        }

        fn handle_message_a(&mut self) -> ShouldTerminate {
            ShouldTerminate::from(true)
        }

        fn handle_message_b(&mut self, params: (u8, String)) -> ShouldTerminate {
            ShouldTerminate::from(false)
        }

        fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
            ShouldTerminate::from(false)
        }
    }

    impl TestWorker1 {
        fn new(zmq_ctx: zmq::Context, address: &Address, payload: u64) -> Self {
            Self {
                inbox: Inbox::new(zmq_ctx, address),
                payload,
            }
        }
    }

    #[test]
    fn create_handmade_worker() {
        let ctx = zmq::Context::new();
        TestWorker1::new(ctx, &Address::from("inproc://worker1"), 42);
    }

    #[test]
    fn run_handmade_worker() {
        let ctx = zmq::Context::new();
        let address = Address::from("inproc://worker1");

        let ctx_copy = ctx.clone();
        let address_copy = address.clone();
        let thread_handle = std::thread::spawn(move || {
            let mut worker = TestWorker1::new(ctx_copy, &address_copy, 42);

            worker.run();
        });

        let outbox = Outbox::new(ctx, &address);
        let message = FirstMessageType::MessageA;
        outbox.send(&message);
        thread_handle.join().expect("Cannot join worker thread");
    }

    #[derive(Serialize, Deserialize, Actor)]
    #[worker(TestWorker2)]
    enum TestWorker2Message {
        MessageA,
        // MessageB(u8, String),
        MessageC { c_foo: u64, c_bar: String },
    }

    struct TestWorker2 {
        // ToDo: is there any way to automate this or, at least, enforce?
        inbox: Inbox,
    }

    impl TestWorker2 {
        fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
            Self {
                inbox: Inbox::new(zmq_ctx, address),
            }
        }

        fn handle_message_a(&mut self) -> ShouldTerminate {
            println!("Received message A");
            ShouldTerminate::from(true)
        }

        fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
            println!("Received message C: c_foo: {}, c_bar: {}", c_foo, c_bar);
            ShouldTerminate::from(false)
        }
    }

    #[test]
    fn create_derived_worker() {
        let ctx = zmq::Context::new();
        TestWorker2::new(ctx, &Address::from("inproc://worker2"));
    }

    #[test]
    fn run_derived_worker() {
        let ctx = zmq::Context::new();
        let address = Address::from("inproc://worker2");

        let ctx_copy = ctx.clone();
        let address_copy = address.clone();
        let thread_handle = std::thread::spawn(move || {
            let mut worker = TestWorker2::new(ctx_copy, &address_copy);

            worker.run();
        });

        let mailbox = Outbox::new(ctx, &address);
        let message = TestWorker2Message::MessageC {
            c_foo: 42,
            c_bar: "hello world".to_owned(),
        };
        mailbox.send(&message);

        let message = TestWorker2Message::MessageA;
        mailbox.send(&message);

        thread_handle.join().expect("Cannot join worker thread");
    }
}
