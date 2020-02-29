use tokio::prelude::*;
use tokio::sync::{oneshot, mpsc};
use std::collections::HashMap;

// TODO: use tokio's lock for avoiding suspend scheduler
use std::sync::RwLock;

pub enum Command {
    Read,
    Write
}

pub enum LastParam {
    Read(oneshot::Sender<Option<i32>>),
    Write(i32)
}

type Channel = (Command, i32, LastParam);
type Receiver = mpsc::Receiver<Channel>;
type Sender = mpsc::Sender<Channel>;

struct TMapTable {
    table: Vec<Sender>,
    partion: i32
}

pub struct TMap {
    tables: HashMap<String, TMapTable>,
}

pub async fn receive(mut receiver: Receiver) {
    let mut map: HashMap<i32, i32> = HashMap::new();

    while let Some(request) = receiver.recv().await {
        match request {
            (Command::Read, key, LastParam::Read(resp)) => {
                let result = map.get(&key);
                match result {
                    Some(r) => resp.send(Some(r.clone())),
                    None => resp.send(None)
                };
                ()
            },
            (Command::Write, key, LastParam::Write(val)) => {
                map.insert(key, val);
                ()
            },
            _ => ()
        };
    }
}

impl TMap {
    pub fn new() -> TMap {
        TMap {
            tables: HashMap::new(),
        }
    }

    pub fn create_table_with_partion(&mut self, name: &str, partion: i32) -> Vec<Receiver> {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        for _ in 0..partion {
            let (mut sender, mut receiver) = mpsc::channel::<Channel>(100);
            senders.push(sender);
            receivers.push(receiver);
        }
        let table = TMapTable{table: senders, partion: partion};
        self.tables.insert(name.to_string(), table);
       receivers
    }

    pub fn get_sender(&self, name: &str, key: i32) -> Sender {
        let partion = self.tables.get(name).unwrap().partion;
        let index = key % partion;
        unsafe { self.tables.get(name).unwrap().table.get_unchecked(index as usize).clone() }
    }
}

pub async fn write(sender: &mut Sender, key: i32, val: i32) {
    if let Err(e) = sender.send((Command::Write, key, LastParam::Write(val))).await {
        // TODO: handle error
        println!("the error {:}", e);
    };
}

pub async fn read(sender: &mut Sender, key: i32, resp_sender: oneshot::Sender<Option<i32>>) {
    if let Err(e) = sender.send((Command::Read, key, LastParam::Read(resp_sender))).await {
      // TODO: handle error
      println!("the error {:}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let tables = RwLock::new(TMap::new());
        let mut rxs = tables.write().unwrap().create_table_with_partion("test", 3);

        for rx in rxs {
            tokio::spawn(receive(rx));
        }

        tokio::spawn(async move {
            let sender = tables.read().unwrap().get_sender("test", 10);
            write(&mut sender.clone(), 0, 11).await;

            let (resp_tx, resp_rx) = oneshot::channel::<Option<i32>>();
            read(&mut sender.clone(), 0, resp_tx).await;
            let v = resp_rx.await.unwrap();
            assert_eq!(v, Some(11));

            let (resp_tx, resp_rx) = oneshot::channel::<Option<i32>>();
            read(&mut sender.clone(), 1123, resp_tx).await;

            let v = resp_rx.await.unwrap();
            assert_eq!(v, None);

            for i in (1..1000) {
                write(&mut sender.clone(), i, i + 7).await;

                let (resp_tx, resp_rx) = oneshot::channel::<Option<i32>>();
                read(&mut sender.clone(), i, resp_tx).await;
                let v = resp_rx.await.unwrap();
                assert_eq!(v, Some(i + 7));
            }
        }).await;
    }
}
