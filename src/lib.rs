use bincode::config;
use serde::{Deserialize, Serialize};
use std::io::Write;

pub use custom_derive::actor_message;

const ADDRESS_LENGTH: usize = 32;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Address {
    // #[serde(with = "serde_bytes")]
    conn_string: [u8; ADDRESS_LENGTH],
}

// ToDo: probably these guys should be newtypes and not aliases?
pub type DestAddress = Address;
pub type SourceAddress = Address;

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub enum AddressType {
    // ToDo: probably this should be renamed so that it's clear
    // that it's an inproc address, not a local IP one
    Local,
    Remote,
}

// ToDo: impl From<std::net::IpAddr>
impl Address {
    pub fn new(address_type: AddressType) -> Self {
        let mut conn_string = [0 as u8; ADDRESS_LENGTH];

        match address_type {
            AddressType::Local => {
                write!(
                    &mut conn_string[..],
                    "inproc://{}",
                    silly_names::make_name(ADDRESS_LENGTH - "inproc://".len())
                )
                .expect("Cannot create address");

                Self { conn_string }
            }
            AddressType::Remote => {
                write!(
                    &mut conn_string[..],
                    "tcp://127.0.0.1:{}",
                    5000 + rand::random::<u64>() % 5000
                )
                .expect("Cannot create address");

                Self { conn_string }
            }
        }
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ShouldBlock(bool);

impl From<bool> for ShouldBlock {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

fn truncate_byte_array_string(bytes: &[u8]) -> &str {
    // zmq converts our &str into a CString so it gets mad when
    // we pass a string with zero bytes
    let zero_byte_position = bytes.iter().position(|&v| v == 0).unwrap_or(bytes.len());
    let truncated_bytes = &bytes[..zero_byte_position];
    std::str::from_utf8(truncated_bytes).expect("Truncated string is not valid utf-8")
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
            .bind(truncate_byte_array_string(&address.conn_string))
            .expect("Cannot connect control socket");

        Self { control_socket }
    }

    pub fn receive(&self, should_block: ShouldBlock) -> Option<Vec<u8>> {
        match self.control_socket.recv_bytes(if should_block.0 {
            0
        } else {
            // This is actually bad since we should have used ZMQ_NOBLOCK here,
            // but zmq crate does not expose it :( Fortunately, integer values
            // of these enum variants coincide
            zmq::DONTWAIT
        }) {
            Ok(bytes) => Some(bytes),
            Err(err) => match err {
                zmq::Error::EAGAIN => None,
                _ => panic!("Actor failed to receive message"),
            },
        }
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
            .connect(truncate_byte_array_string(&dest_address.conn_string))
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
    pub fn open(mut self) -> (DestAddress, SourceAddress, Vec<u8>) {
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

    use crate::{Address, AddressType, Envelope, Inbox, Outbox, ShouldBlock, ShouldTerminate};
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
            let envelope = Envelope::from(
                self.inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
            let envelope = Envelope::from(
                inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
            let envelope = Envelope::from(
                inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
            let envelope = Envelope::from(
                self.inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
    fn run_derived_worker_local() {
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
            let envelope = Envelope::from(
                inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
            let envelope = Envelope::from(
                inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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

    #[test]
    fn run_derived_worker_remote() {
        let ctx = zmq::Context::new();

        let first_worker_address = Address::new(AddressType::Remote);
        eprintln!("first_worker_address: {}", &first_worker_address);

        let second_worker_address = Address::new(AddressType::Remote);
        eprintln!("second_worker_address: {}", &second_worker_address);

        let spawner_address = Address::new(AddressType::Remote);
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
            let envelope = Envelope::from(
                inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
            let envelope = Envelope::from(
                inbox
                    .receive(ShouldBlock::from(true))
                    .expect("Cannot receive message"),
            );
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
