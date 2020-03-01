use bencher::{black_box, Bencher};
use std::sync::{Arc, RwLock};
use tokio::prelude::*;
use tokio::sync::oneshot;

async fn do_read(tables: Arc<RwLock<tmap::TMap>>, table_name: &str, start: i32, end: i32) {
    for i in (start..end) {
        let (resp_tx, resp_rx) = oneshot::channel::<Option<i32>>();
        let sender = tables.read().unwrap().get_sender(table_name, i);
        tmap::read(&mut sender.clone(), i, resp_tx).await;
        let v = resp_rx.await.unwrap();
    }
}

fn read(bench: &mut Bencher) {
    let tables = Arc::new(RwLock::new(tmap::TMap::new()));
    let threads_count = 8;
    let table_name = "bench";
    let total_count = 32000;
    let mut rxs = tables.write().unwrap().create_table_with_partion(table_name, threads_count as i32);

    let mut rt = tokio::runtime::Builder::new()
        .core_threads(threads_count)
        .threaded_scheduler()
        .build()
        .unwrap();

    rt.spawn(async move {
        for rx in rxs {
            tokio::spawn(tmap::receive(rx));
        }
    });


    let tables_inner = tables.clone();
    rt.block_on(async move {
        for i in (0..total_count) {
            let sender = tables_inner.read().unwrap().get_sender(table_name, i);
            tmap::write(&mut sender.clone(), i, i).await;
        }
    });

    let tables_inner2 = tables.clone();

    let task_counts = threads_count as i32;
    let count_for_each_task = total_count / task_counts ;

    bench.iter(move || {
        let tables_inner3 = tables_inner2.clone();
        rt.block_on(async move {
            let mut futures = Vec::new();

            for i in (0..task_counts) {
                let start = i * count_for_each_task;
                let end = (i + 1) * count_for_each_task;
                let f = tokio::spawn(do_read(tables_inner3.clone(), table_name, start, end));
                futures.push(f);
            }

            for f in futures {
                f.await;
            }
        })
    })
}

bencher::benchmark_group!(
    lib,
    read
);

bencher::benchmark_main!(lib);
