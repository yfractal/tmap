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
    Read(oneshot::Sender<Option<u64>>),
    Write(u64)
}

type Channel = (Command, u64, LastParam);
type Receiver = mpsc::Receiver<Channel>;
type Sender = mpsc::Sender<Channel>;

pub struct TMap {
    tables: HashMap<String, Sender>,
}

pub async fn receive(mut receiver: Receiver) {
    let mut map: HashMap<u64, u64> = HashMap::new();

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

    pub fn create_table(&mut self, name: &str) -> Receiver {
        let (mut sender, mut receiver) = mpsc::channel::<Channel>(100);
        self.tables.insert(name.to_string(), sender);
        receiver
    }

    pub fn get_sender(&self, name: &str) -> Sender{
        self.tables.get(name).unwrap().clone()
    }
}

pub async fn write(sender: &mut Sender, key: u64, val: u64) {
    if let Err(e) = sender.send((Command::Write, key, LastParam::Write(val))).await {
        // TODO: handle error
        println!("the error {:}", e);
    };
}

pub async fn read(sender: &mut Sender, key: u64, resp_sender: oneshot::Sender<Option<u64>>) {
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
        let mut rx = tables.write().unwrap().create_table("test");
        tokio::spawn(receive(rx));

        tokio::spawn(async move {
            let sender = tables.read().unwrap().get_sender("test");
            write(&mut sender.clone(), 10 as u64, 11 as u64).await;

            let (resp_tx, resp_rx) = oneshot::channel::<Option<u64>>();
            read(&mut sender.clone(), 10 as u64, resp_tx).await;

            let v = resp_rx.await.unwrap();
            assert_eq!(v, Some(11));

            let (resp_tx, resp_rx) = oneshot::channel::<Option<u64>>();
            read(&mut sender.clone(), 1123 as u64, resp_tx).await;

            let v = resp_rx.await.unwrap();
            assert_eq!(v, None);
        }).await;
    }
}
