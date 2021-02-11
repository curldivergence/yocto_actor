use bincode::config;
use serde::{Deserialize, Serialize};
use std::io::Write;

pub use custom_derive::actor_message;

const ADDRESS_LENGTH: usize = 32;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Address {
    // #[serde(with = "serde_bytes")]
    conn_string: [u8; ADDRESS_LENGTH],
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub enum AddressType {
    Local,
    Remote,
}

impl Address {
    pub fn new(address_type: AddressType) -> Self {
        // zmq doesn't like zero bytes in connection strings,
        // so let's pad them with 'x' (ascii code 120)
        let mut conn_string = [120 as u8; ADDRESS_LENGTH];
        write!(
            &mut conn_string[..],
            "{}://{:X}",
            match address_type {
                AddressType::Local => "inproc",
                AddressType::Remote => "tcp",
            },
            rand::random::<u64>()
        )
        .expect("Cannot create address");

        Self { conn_string }
    }

    pub fn get_type(&self) -> AddressType {
        match self.conn_string[..3] {
            // 'inp'
            [0x69, 0x6e, 0x70] => AddressType::Local,
            // 'tcp'
            [0x74, 0x63, 0x70] => AddressType::Remote,
            _ => panic!("Address connection string is malformed"),
        }
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.conn_string)
            .expect("Address connection string is not valid utf-8")
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
            .bind(address.as_str())
            .expect("Cannot connect control socket");

        Self { control_socket }
    }

    pub fn receive(&self) -> Vec<u8> {
        self.control_socket
            .recv_bytes(0)
            .expect("Actor cannot receive message bytes")
    }
}

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
            .connect(dest_address.as_str())
            .expect("Cannot connect control socket");

        Self {
            control_socket,
            dest_address: dest_address.clone(),
            source_address: source_address.clone(),
        }
    }

    pub fn send<MessageType: serde::Serialize>(&self, message: &MessageType) {
        let mut message_bytes = bincode::serialize(message).expect("Cannot serialize message");
        message_bytes.extend(self.source_address.conn_string.iter());
        message_bytes.extend(self.dest_address.conn_string.iter());

        self.control_socket
            .send(&message_bytes, 0)
            .expect("Cannot send message to worker");
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Envelope(Vec<u8>);

impl Envelope {
    pub fn open(mut self) -> (Address, Address, Vec<u8>) {
        let mut dest_address = [0 as u8; ADDRESS_LENGTH];
        for (idx, byte) in self.0.drain(self.0.len() - ADDRESS_LENGTH..).enumerate() {
            dest_address[idx] = byte;
        }

        let mut source_address = [0 as u8; ADDRESS_LENGTH];
        for (idx, byte) in self.0.drain(self.0.len() - ADDRESS_LENGTH..).enumerate() {
            source_address[idx] = byte;
        }

        (
            Address {
                conn_string: dest_address,
            },
            Address {
                conn_string: source_address,
            },
            self.0,
        )
    }
}

impl From<Vec<u8>> for Envelope {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
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

#[cfg(test)]
mod tests {
    use std::unimplemented;

    use crate::{Address, AddressType, Envelope, Inbox, Outbox, ShouldTerminate};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    enum FirstMessageType {
        MessageA,
        MessageB { c_foo: u64, c_bar: String },
    }

    trait FirstMessageTypeHandler {
        fn pre_run(&mut self) {}
        fn post_run(&mut self) {}

        fn receive(&self) -> FirstMessageType;

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

        fn dispatch_message(&mut self, message: FirstMessageType) -> ShouldTerminate {
            match message {
                FirstMessageType::MessageA => self.handle_message_a(),
                FirstMessageType::MessageB { c_foo, c_bar } => self.handle_message_b(c_foo, c_bar),
            }
        }

        // Event handlers

        fn handle_message_a(&mut self) -> ShouldTerminate;
        fn handle_message_b(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate;
    }

    struct HandmadeWorker {
        inbox: Inbox,
        next_stage_outbox: Outbox,
        payload: u64,
    }

    impl FirstMessageTypeHandler for HandmadeWorker {
        fn receive(&self) -> FirstMessageType {
            let envelope = Envelope::from(self.inbox.receive());
            let (_, _, message_bytes) = envelope.open();

            let message: FirstMessageType =
                bincode::deserialize(&message_bytes).expect("Actor cannot deserialize envelope");

            message
        }

        fn handle_message_a(&mut self) -> ShouldTerminate {
            self.next_stage_outbox.send(&FirstMessageType::MessageA);
            ShouldTerminate::from(true)
        }

        fn handle_message_b(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
            self.next_stage_outbox.send(&FirstMessageType::MessageB {
                c_foo: c_foo + self.payload,
                c_bar: "String payload".to_owned(),
            });
            ShouldTerminate::from(false)
        }

        fn pre_run(&mut self) {
            println!("This is pre_run!");
        }
    }

    impl HandmadeWorker {
        fn new(
            zmq_ctx: zmq::Context,
            worker_address: &Address,
            next_stage_address: &Address,
            payload: u64,
        ) -> Self {
            Self {
                inbox: Inbox::new(zmq_ctx.clone(), worker_address),
                next_stage_outbox: Outbox::new(zmq_ctx, next_stage_address, worker_address),
                payload,
            }
        }
    }

    #[test]
    fn run_handmade_worker() {
        let ctx = zmq::Context::new();

        let first_worker_address = Address::new(AddressType::Local);
        eprintln!("first_worker_address: {}", &first_worker_address);

        let second_worker_address = Address::new(AddressType::Local);
        eprintln!("second_worker_address: {}", &second_worker_address);

        let spawner_address = Address::new(AddressType::Local);
        eprintln!("spawner_address: {}", &spawner_address);

        let inbox = Inbox::new(ctx.clone(), &spawner_address);

        let first_worker_thread = {
            let ctx_copy = ctx.clone();
            let first_worker_address_copy = first_worker_address.clone();
            let second_worker_address_copy = second_worker_address.clone();

            std::thread::spawn(move || {
                let mut first_worker = HandmadeWorker::new(
                    ctx_copy,
                    &first_worker_address_copy,
                    &second_worker_address_copy,
                    42,
                );

                first_worker.run();
            })
        };

        let second_worker_thread = {
            let ctx_copy = ctx.clone();
            let second_worker_address_copy = second_worker_address.clone();
            let spawner_address_copy = spawner_address.clone();

            std::thread::spawn(move || {
                let mut second_worker = HandmadeWorker::new(
                    ctx_copy,
                    &second_worker_address_copy,
                    &spawner_address_copy,
                    43,
                );

                second_worker.run();
            })
        };

        let outbox = Outbox::new(ctx.clone(), &first_worker_address, &spawner_address);
        outbox.send(&FirstMessageType::MessageB {
            c_foo: 50,
            c_bar: "Ta-da-da".to_owned(),
        });

        {
            let envelope = Envelope::from(inbox.receive());
            let (_, _, message_bytes) = envelope.open();

            let message: FirstMessageType =
                bincode::deserialize(&message_bytes).expect("Spawner cannot deserialize envelope");

            if let FirstMessageType::MessageB { c_foo, c_bar } = message {
                eprintln!("Spawner received message B: {}", &c_foo);
                assert_eq!(c_foo, 50 + 42 + 43);
            } else {
                panic!("Spawner received wrong message type (expected A, received B)");
            }
        }

        outbox.send(&FirstMessageType::MessageA);

        {
            let envelope = Envelope::from(inbox.receive());
            let (_, _, message_bytes) = envelope.open();

            let message: FirstMessageType =
                bincode::deserialize(&message_bytes).expect("Spawner cannot deserialize envelope");
            match message {
                FirstMessageType::MessageA => {}
                FirstMessageType::MessageB { .. } => {
                    panic!("Spawner received wrong message type (expected B, received A)")
                }
            }
        }

        first_worker_thread
            .join()
            .expect("Cannot join first worker");
        second_worker_thread
            .join()
            .expect("Cannot join second worker");
    }

    use custom_derive::actor_message;
    #[actor_message]
    #[derive(Serialize, Deserialize)]
    enum SecondMessageType {
        MessageA,
        MessageB { c_foo: u64, c_bar: String },
    }

    struct DerivedWorker {
        inbox: Inbox,
        next_stage_outbox: Outbox,
        payload: u64,
    }

    impl SecondMessageTypeHandler for DerivedWorker {
        fn receive(&self) -> SecondMessageType {
            let envelope = Envelope::from(self.inbox.receive());
            let (_, _, message_bytes) = envelope.open();

            let message: SecondMessageType =
                bincode::deserialize(&message_bytes).expect("Actor cannot deserialize envelope");

            message
        }

        fn handle_message_a(&mut self) -> ShouldTerminate {
            self.next_stage_outbox.send(&FirstMessageType::MessageA);
            ShouldTerminate::from(true)
        }

        fn handle_message_b(&mut self, c_foo: u64, c_bar: String) -> ShouldTerminate {
            self.next_stage_outbox.send(&FirstMessageType::MessageB {
                c_foo: c_foo + self.payload,
                c_bar: "String payload".to_owned(),
            });
            ShouldTerminate::from(false)
        }

        fn pre_run(&mut self) {
            println!("This is pre_run!");
        }
    }

    impl DerivedWorker {
        fn new(
            zmq_ctx: zmq::Context,
            worker_address: &Address,
            next_stage_address: &Address,
            payload: u64,
        ) -> Self {
            Self {
                inbox: Inbox::new(zmq_ctx.clone(), worker_address),
                next_stage_outbox: Outbox::new(zmq_ctx, next_stage_address, worker_address),
                payload,
            }
        }
    }

    #[test]
    fn run_derived_worker() {
        let ctx = zmq::Context::new();

        let first_worker_address = Address::new(AddressType::Local);
        eprintln!("first_worker_address: {}", &first_worker_address);

        let second_worker_address = Address::new(AddressType::Local);
        eprintln!("second_worker_address: {}", &second_worker_address);

        let spawner_address = Address::new(AddressType::Local);
        eprintln!("spawner_address: {}", &spawner_address);

        let inbox = Inbox::new(ctx.clone(), &spawner_address);

        let first_worker_thread = {
            let ctx_copy = ctx.clone();
            let first_worker_address_copy = first_worker_address.clone();
            let second_worker_address_copy = second_worker_address.clone();

            std::thread::spawn(move || {
                let mut first_worker = DerivedWorker::new(
                    ctx_copy,
                    &first_worker_address_copy,
                    &second_worker_address_copy,
                    42,
                );

                first_worker.run();
            })
        };

        let second_worker_thread = {
            let ctx_copy = ctx.clone();
            let second_worker_address_copy = second_worker_address.clone();
            let spawner_address_copy = spawner_address.clone();

            std::thread::spawn(move || {
                let mut second_worker = DerivedWorker::new(
                    ctx_copy,
                    &second_worker_address_copy,
                    &spawner_address_copy,
                    43,
                );

                second_worker.run();
            })
        };

        let outbox = Outbox::new(ctx.clone(), &first_worker_address, &spawner_address);
        outbox.send(&FirstMessageType::MessageB {
            c_foo: 50,
            c_bar: "Ta-da-da".to_owned(),
        });

        {
            let envelope = Envelope::from(inbox.receive());
            let (_, _, message_bytes) = envelope.open();

            let message: FirstMessageType =
                bincode::deserialize(&message_bytes).expect("Spawner cannot deserialize envelope");

            if let FirstMessageType::MessageB { c_foo, c_bar } = message {
                eprintln!("Spawner received message B: {}", &c_foo);
                assert_eq!(c_foo, 50 + 42 + 43);
            } else {
                panic!("Spawner received wrong message type (expected A, received B)");
            }
        }

        outbox.send(&FirstMessageType::MessageA);

        {
            let envelope = Envelope::from(inbox.receive());
            let (_, _, message_bytes) = envelope.open();

            let message: FirstMessageType =
                bincode::deserialize(&message_bytes).expect("Spawner cannot deserialize envelope");
            match message {
                FirstMessageType::MessageA => {}
                FirstMessageType::MessageB { .. } => {
                    panic!("Spawner received wrong message type (expected B, received A)")
                }
            }
        }

        first_worker_thread
            .join()
            .expect("Cannot join first worker");
        second_worker_thread
            .join()
            .expect("Cannot join second worker");
    }
}
