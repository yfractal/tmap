use tokio::prelude::*;
use tokio::sync::{oneshot, mpsc};
use std::collections::HashMap;

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
    name: String,
}

impl TMap {
    pub fn new(name: String) -> TMap {
        TMap {
            name: name
        }
    }
}

pub async fn receive(mut receiver: Receiver) {
    let mut map: HashMap<u64, u64> = HashMap::new();

    while let Some(request) = receiver.recv().await {
        match request {
            (Command::Read, key, LastParam::Read(resp)) => {
                let result = map.get(&key);
                match result {
                    Some(r) => resp.send(Some(r.clone())).unwrap(),
                    None => resp.send(None).unwrap()
                }
            },
            (Command::Write, key, LastParam::Write(val)) => {
                map.insert(key, val);
                ()
            },
            _ => ()
        };
    }
}

pub async fn write(sender: &mut Sender, key: u64, val: u64) {
    if let Err(e) = sender.send((Command::Write, key, LastParam::Write(val))).await {
        println!("the error {:}", e);
    };
}

pub async fn read(sender: &mut Sender, key: u64, resp_sender: oneshot::Sender<Option<u64>>) {
    if let Err(e) = sender.send((Command::Read, key, LastParam::Read(resp_sender))).await {
      println!("the error {:}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let (mut sender, mut rx) = mpsc::channel::<Channel>(100);

        tokio::spawn(async move {
            write(&mut sender, 10 as u64, 11 as u64).await;

            // read
            let (resp_tx, resp_rx) = oneshot::channel::<Option<u64>>();
            read(&mut sender, 10 as u64, resp_tx).await;

            let v = resp_rx.await.unwrap();
            println!("got val {:?}", v);
        });

        let r = tokio::spawn(receive(rx));
        r.await;

        assert_eq!(1, 1);
    }

}
