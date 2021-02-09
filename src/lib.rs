use bincode::config;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::mpsc;

pub use custom_derive::actor_message;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Address {
    conn_string: String,
}

impl From<&str> for Address {
    fn from(conn_string: &str) -> Self {
        Self {
            conn_string: conn_string.to_owned(),
        }
    }
}

pub struct Inbox {
    control_socket: zmq::Socket,
}

impl Inbox {
    pub fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
        let control_socket = zmq_ctx
            .socket(zmq::PULL)
            .expect("Cannot create control socket");
        control_socket
            .bind(&address.conn_string)
            .expect("Cannot connect control socket");

        Self { control_socket }
    }

    pub fn receive(&self) -> Vec<u8> {
        self.control_socket
            .recv_bytes(0)
            .expect("Actor cannot receive message bytes")
    }
}

// ToDo: store receiver name or some kind of id?
pub struct Outbox {
    control_socket: zmq::Socket,
    dest_address: Address,
    source_address: Address,
}

impl Outbox {
    // ToDo: yeah, this duplication is sad, but will do for now
    pub fn new(zmq_ctx: zmq::Context, dest_address: &Address, source_address: &Address) -> Self {
        let control_socket = zmq_ctx
            .socket(zmq::PUSH)
            .expect("Cannot create control socket");
        control_socket
            .connect(&dest_address.conn_string)
            .expect("Cannot connect control socket");

        Self {
            control_socket,
            dest_address: dest_address.clone(),
            source_address: source_address.clone(),
        }
    }

    pub fn send<MessageType: serde::Serialize>(&self, message: MessageType) {
        let message_bytes = bincode::serialize(message).expect("Cannot serialize message");
        let envelope = Envelope {
            to: self.dest_address,
            from: self.source_address,
            payload,
        };
        self.control_socket
            .send(&message_bytes, 0)
            .expect("Cannot send message to worker");
    }
}

pub struct Envelope {
    pub from: Address,
    pub to: Address,
    pub payload: Vec<u8>,
}

// pub struct ChannelInbox<MessageType> {
//     receiver: mpsc::Receiver<MessageType>,
// }

// impl<MessageType> ChannelInbox<MessageType> {
//     fn receive(&self) -> MessageType {
//         self.receiver.recv().expect("Cannot receive from inbox")
//     }
// }

// #[derive(Clone)]
// pub struct ChannelOutbox<MessageType> {
//     sender: mpsc::Sender<MessageType>,
// }

// impl<MessageType> ChannelOutbox<MessageType> {
//     fn send(&self, message: MessageType) {
//         self.sender
//             .send(message)
//             .expect("Cannot send message to channel")
//     }
// }

// pub fn make_channel_pipe<MessageType>() -> (ChannelOutbox<MessageType>, ChannelInbox<MessageType>) {
//     let (tx, rx) = mpsc::channel();
//     (ChannelOutbox { sender: tx }, ChannelInbox { receiver: rx })
// }

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

#[cfg(test)]
mod tests {
    use std::unimplemented;

    use crate::{Address, Inbox, Outbox, ShouldTerminate};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    enum FirstMessageType {
        MessageA,
        MessageB(u8, String),
        MessageC { c_foo: u64, c_bar: String },
    }

    trait FirstMessageTypeHandler {
        fn pre_run(&mut self) {}
        fn post_run(&mut self) {}

        fn receive(&self) -> FirstMessageType;

        fn run(&mut self) {
            loop {
                self.pre_run();

                let message = self.receive();
                if self.dispatch_message(message).0 {
                    break;
                }

                self.post_run();
            }
        }

        fn dispatch_message(&mut self, message: FirstMessageType) -> ShouldTerminate {
            match message {
                FirstMessageType::MessageA => self.handle_message_a(),
                FirstMessageType::MessageB(val1, val2) => self.handle_message_b((val1, val2)),
                FirstMessageType::MessageC { c_foo, c_bar } => self.handle_message_c(c_foo, c_bar),
            }
        }

        // Event handlers

        fn handle_message_a(&mut self) -> ShouldTerminate;
        fn handle_message_b(&mut self, data: (u8, String)) -> ShouldTerminate;
        fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate;
    }

    struct HandmadeWorker {
        inbox: Inbox,
        payload: u64,
    }

    impl FirstMessageTypeHandler for HandmadeWorker {
        fn receive(&self) -> FirstMessageType {
            let message_bytes = self.inbox.receive();
            let message: FirstMessageType =
                bincode::deserialize(&message_bytes).expect("Actor cannot deserialize message");
            message
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

        fn pre_run(&mut self) {
            println!("This is pre_run!");
        }
    }

    impl HandmadeWorker {
        fn new(zmq_ctx: zmq::Context, address: &Address, payload: u64) -> Self {
            Self {
                inbox: Inbox::new(zmq_ctx, address),
                payload,
            }
        }
    }

    #[test]
    fn run_handmade_worker() {
        let ctx = zmq::Context::new();
        let address = Address::from("inproc://worker1");

        let ctx_copy = ctx.clone();
        let address_copy = address.clone();
        let thread_handle = std::thread::spawn(move || {
            let mut worker = HandmadeWorker::new(ctx_copy, &address_copy, 42);

            worker.run();
        });

        let outbox = Outbox::new(ctx, &address);
        let message = FirstMessageType::MessageA;
        outbox.send(&message);
        thread_handle.join().expect("Cannot join worker thread");
    }

    // struct HandmadeChannelWorker<MessageType> {
    //     inbox: ChannelInbox<MessageType>,
    // }

    // impl FirstMessageTypeHandler for HandmadeChannelWorker<FirstMessageType> {
    //     fn receive(&self) -> FirstMessageType {
    //         self.inbox.receive()
    //     }

    //     fn handle_message_a(&mut self) -> ShouldTerminate {
    //         ShouldTerminate::from(true)
    //     }

    //     fn handle_message_b(&mut self, params: (u8, String)) -> ShouldTerminate {
    //         ShouldTerminate::from(false)
    //     }

    //     fn handle_message_c(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
    //         ShouldTerminate::from(false)
    //     }

    //     fn pre_run(&mut self) {
    //         println!("This is pre_run!");
    //     }
    // }

    // impl<MessageType> HandmadeChannelWorker<MessageType> {
    //     fn new() -> (Self, ChannelOutbox<MessageType>) {
    //         let (tx, rx) = make_channel_pipe();

    //         return (Self { inbox: rx }, tx);
    //     }
    // }

    // #[test]
    // fn run_channel_worker() {
    //     let (mut worker, outbox) = HandmadeChannelWorker::new();

    //     let thread_handle = std::thread::spawn(move || {
    //         worker.run();
    //     });

    //     let message = FirstMessageType::MessageA;
    //     outbox.send(message);
    //     thread_handle.join().expect("Cannot join worker thread");
    // }

    use custom_derive::actor_message;
    #[actor_message]
    #[derive(Serialize, Deserialize)]
    enum SecondMessageType {
        MessageA,
        // MessageB(u8, String),
        MessageC { c_foo: u64, c_bar: String },
    }

    struct DerivedZmqWorker {
        inbox: Inbox,
    }

    impl SecondMessageTypeHandler for DerivedZmqWorker {
        fn receive(&self) -> Vec<u8> {
            self.inbox.receive()
        }

        fn pre_run(&mut self) {
            println!("This is pre_run!");
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

    impl DerivedZmqWorker {
        fn new(zmq_ctx: zmq::Context, address: &Address) -> Self {
            Self {
                inbox: Inbox::new(zmq_ctx, address),
            }
        }
    }

    #[test]
    fn run_derived_worker() {
        let ctx = zmq::Context::new();
        let address = Address::from("inproc://worker2");

        let ctx_copy = ctx.clone();
        let address_copy = address.clone();
        let thread_handle = std::thread::spawn(move || {
            let mut worker = DerivedZmqWorker::new(ctx_copy, &address_copy);

            worker.run();
        });

        let mailbox = Outbox::new(ctx, &address);
        let message = SecondMessageType::MessageC {
            c_foo: 42,
            c_bar: "hello world".to_owned(),
        };
        mailbox.send(&message);

        let message = SecondMessageType::MessageA;
        mailbox.send(&message);

        thread_handle.join().expect("Cannot join worker thread");
    }
}
