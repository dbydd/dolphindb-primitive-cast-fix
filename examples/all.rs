use dolphindb::{
    client::{ClientBuilder, TableWriter},
    error::Error,
    stream_client::{request::Request, subscriber::SubscriberBuilder},
    types::*,
};
use std::collections::HashMap;
use chrono::{NaiveDateTime, Utc};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use futures::StreamExt;

// ==================== type_conversion.rs ====================
async fn test_type_conversion() {
    let conn_str = std::env::var("DOLPHIN_DB_CONNECT").unwrap_or_else(|_| {
        panic!("请设置 DOLPHIN_DB_CONNECT 环境变量，格式：username@password@host:port")
    });
    let parts: Vec<&str> = conn_str.split('@').collect();
    if parts.len() != 3 {
        panic!("无效的连接字符串格式，应为 username@password@host:port");
    }
    let (username, password, host_port) = (parts[0], parts[1], parts[2]);
    
    let mut builder = ClientBuilder::new(host_port);
    builder.with_auth((username, password));
    let mut client = builder.connect().await.unwrap();
    let mut variables: HashMap<String, ConstantImpl> = HashMap::new();

    // 基本类型示例
    variables.insert("Int".to_owned(), Int::new(1).into());
    variables.insert("Double".to_owned(), Double::new(1.0).into());
    variables.insert(
        "String".to_owned(),
        DolphinString::new("str".to_owned()).into(),
    );

    // 数值类型
    variables.insert("myVoid".to_owned(), Void::new(()).into());
    variables.insert("myBool".to_owned(), Bool::new(true).into());
    variables.insert("myChar".to_owned(), Char::new('a' as i8).into());
    variables.insert("myShort".to_owned(), Short::new(1i16).into());
    variables.insert("myInt".to_owned(), Int::new(2i32).into());
    variables.insert("myLong".to_owned(), Long::new(3i64).into());
    variables.insert("myFloat".to_owned(), Float::new(1.1f32).into());
    variables.insert("myDouble".to_owned(), Double::new(1.2f64).into());
    variables.insert(
        "myDecimal32".to_owned(),
        Decimal32::from_raw(100i32, 1).unwrap().into(),
    );
    variables.insert(
        "myDecimal64".to_owned(),
        Decimal64::from_raw(10000i64, 2).unwrap().into(),
    );
    variables.insert(
        "myDecimal128".to_owned(),
        Decimal128::from_raw(1000000i128, 3).unwrap().into(),
    );

    // 时间类型
    let unix_timestamp = 1735660800_000i64;
    variables.insert(
        "myDate".to_owned(),
        Date::from_raw(unix_timestamp / 86400_000).unwrap().into(),
    );
    variables.insert(
        "myDateTime".to_owned(),
        DateTime::from_raw((unix_timestamp / 1000) as i32)
            .unwrap()
            .into(),
    );
    variables.insert(
        "myTimestamp".to_owned(),
        Timestamp::from_raw(unix_timestamp).unwrap().into(),
    );
    variables.insert(
        "myNanoTimestamp".to_owned(),
        NanoTimestamp::from_raw(unix_timestamp * 1000_000)
            .unwrap()
            .into(),
    );
    variables.insert(
        "myMonth".to_owned(),
        Month::from_ym(2025, 1).unwrap().into(),
    );
    variables.insert(
        "myMinute".to_owned(),
        Minute::from_hm(17, 30).unwrap().into(),
    );
    variables.insert(
        "mySecond".to_owned(),
        Second::from_hms(17, 30, 0).unwrap().into(),
    );
    variables.insert(
        "myTime".to_owned(),
        Time::from_hms_milli(17, 30, 0, 100).unwrap().into(),
    );
    variables.insert(
        "myNanoTime".to_owned(),
        NanoTime::from_hms_nano(17, 30, 0, 100_000_000u32)
            .unwrap()
            .into(),
    );
    variables.insert(
        "myDateHour".to_owned(),
        DateHour::from_ymd_h(2025, 1, 1, 17).unwrap().into(),
    );

    // 其他类型
    variables.insert(
        "myString".to_owned(),
        DolphinString::new("str".to_owned()).into(),
    );
    variables.insert("myBlob".to_owned(), Blob::new(vec![b'a']).into());
    variables.insert("mySymbol".to_owned(), Symbol::new("1".to_owned()).into());

    client.upload(&variables).await.unwrap();

    let vars = [
        "myBool", "myChar", "myShort", "myInt", "myLong", "myFloat", "myDouble",
        "myDecimal32", "myDecimal64", "myDecimal128", "myDate", "myTimestamp",
        "myNanoTimestamp", "myDateTime", "myMonth", "mySecond", "myTime",
        "myNanoTime", "myDateHour", "myString", "myBlob", "mySymbol",
    ];
    for var in vars {
        println!(
            "{}: {}",
            var,
            client.run_script(var).await.unwrap().unwrap()
        );
    }
}

// ==================== table_insert.rs ====================
async fn test_table_insert() -> Result<(), Error> {
    let conn_str = std::env::var("DOLPHIN_DB_CONNECT").unwrap_or_else(|_| {
        panic!("请设置 DOLPHIN_DB_CONNECT 环境变量，格式：username@password@host:port")
    });
    let parts: Vec<&str> = conn_str.split('@').collect();
    if parts.len() != 3 {
        panic!("无效的连接字符串格式，应为 username@password@host:port");
    }
    let (username, password, host_port) = (parts[0], parts[1], parts[2]);
    
    let mut builder = ClientBuilder::new(host_port);
    builder.with_auth((username, password));
    let mut client = builder.connect().await.unwrap();

    let mut prices = DoubleArrayVector::new();
    let price1 = vec![1.1, 2.2, 3.3];
    prices.push(price1);

    // 插入单行
    let c_int = ConstantImpl::from(Int::new(1));
    let c_double_array_vector = ConstantImpl::Vector(VectorImpl::from(prices.clone()));
    let res = client
        .run_function("tableInsert{testTable}", &[c_int, c_double_array_vector])
        .await
        .unwrap()
        .unwrap();
    println!("单行插入结果: {res}");

    // 插入表格
    let price2 = vec![4.4, 5.5];
    prices.push(price2);
    let v_int = IntVector::from_raw(&[2, 3]).into();
    let v_double_array_vector = VectorImpl::from(prices);
    let mut builder = TableBuilder::new();
    builder.with_name("my_table".to_string());
    builder.with_contents(
        vec![v_int, v_double_array_vector],
        vec!["volume".to_string(), "price".to_string()],
    );
    let table = builder.build().unwrap();
    let res = client
        .run_function("tableInsert{testTable}", &[table.into()])
        .await?
        .unwrap();
    println!("表格插入结果: {res}");
    Ok(())
}

// ==================== any_vector.rs ====================
async fn test_any_vector() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let mut v: Vector<Any> = Vector::new();
    v.push_raw(Int::new(1).into());

    let c: ConstantImpl = VectorImpl::Long(Vector::from_raw(&[1.into(), 2.into()])).into();
    v.push(c.into());
    let res = client
        .run_function("max", &vec![ConstantImpl::Vector(v.into())])
        .await
        .unwrap();

    if let Some(ref c) = res {
        println!("Any向量最大值: {}", c);
    }
}

// ==================== subscribe.rs ====================
async fn test_subscribe(action: String) {
    let mut req = Request::new("shared_stream_table".into(), action);
    req.with_offset(0);
    req.with_auth(("admin", "123456"));

    let mut builder = SubscriberBuilder::new();
    let mut subscriber = builder
        .subscribe("127.0.0.1:8848", req)
        .await
        .unwrap()
        .skip(3)
        .take(3);

    // 单消息处理
    while let Some(msg) = subscriber.next().await {
        println!(
            "主题: {}, 偏移量: {}, 内容: {}",
            msg.topic(),
            msg.offset(),
            msg.msg()
        );
    }

    // 批量处理
    let mut batch = Vec::with_capacity(1024);
    let throttle = Duration::from_millis(100);
    loop {
        tokio::select! {
            Some(msg) = subscriber.next() => {
                batch.push(msg);
                if batch.len() == batch.capacity() {
                    println!("处理 {} 条消息", batch.len());
                    batch.clear();
                }
            }
            _ = tokio::time::sleep(throttle) => {
                println!("处理 {} 条消息", batch.len());
                batch.clear();
            }
        }
    }
}

// ==================== quick_start.rs ====================
async fn test_quick_start() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    // 执行脚本
    let res = client.run_script("a = pair(`a, `b)").await.unwrap();
    if let None = res {
        println!("脚本无返回值");
    }

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("获取变量: {}", c);
    }

    // 执行函数
    let ver = client.run_function("version", &[]).await.unwrap().unwrap();
    println!("版本: {ver}");
    
    let typestr = client
        .run_function("typestr", &[res.clone().unwrap()])
        .await
        .unwrap()
        .unwrap();
    println!("类型: {typestr}");
    
    let sum = client
        .run_function("add", &[Int::new(1).into(), Int::new(2).into()])
        .await
        .unwrap()
        .unwrap();
    println!("求和结果: {sum}");

    // 上传数据
    let mut variables = HashMap::new();
    variables.insert("a".to_string(), res.unwrap().clone());
    client.upload(&variables).await.unwrap();
}

// ==================== table_writer.rs ====================
async fn test_table_writer() -> Result<(), Error> {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    // 创建流表
    let stream_table = "depthStreamTable";
    let script = format!(
        r#"
        colNames = ["event_time", "event_time2", "symbol", "event_id", "prices"]
        colTypes = [TIMESTAMP, TIMESTAMP, SYMBOL, LONG, DOUBLE[]]

        if (!existsStreamTable("{stream_table}")) {{
            enableTableShareAndCachePurge(streamTable(1000000:0, colNames, colTypes), "{stream_table}", 1000000)
        }}
    "#
    );
    client.run_script(&script).await.unwrap();

    // 生成测试数据
    let event = TickerEvent {
        event_time: Utc::now().timestamp_millis(),
        event_time2: Utc::now().naive_utc(),
        symbol: "BTCUSDT".into(),
        event_id: 1000,
        prices: vec![5000.0; 100],
    };

    let (tx, mut rx) = mpsc::unbounded_channel::<TickerEvent>();
    let symbol_number = 500;
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    let event1 = event.clone();
    let event2 = event.clone();

    // 数据源1
    tokio::spawn(async move {
        loop {
            for _ in 0..symbol_number {
                let _ = tx1.send(event1.clone());
            }
            sleep(Duration::from_millis(1000 / 20)).await;
        }
    });

    // 数据源2
    tokio::spawn(async move {
        loop {
            for _ in 0..symbol_number {
                let _ = tx2.send(event2.clone());
            }
            sleep(Duration::from_millis(1000 / 10)).await;
        }
    });

    // 写入器
    tokio::spawn(async move {
        let mut inserted = 0usize;
        let mut writer = TableWriter::new(client, stream_table, 512).await;
        while let Some(event) = rx.recv().await {
            let mut row = build_table_row(&event);
            if let Err(e) = writer.append_row(&mut row).await {
                eprintln!("插入失败: {:?}", e);
            }
            inserted += 1;
            if inserted % 10000 == 0 {
                println!("已插入 {} 行，缓冲中 {} 行", inserted, rx.len());
            }
        }
    });

    Ok(())
}

// ==================== symbol_vector.rs ====================
async fn test_symbol_vector() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let res = client.run_script("a = symbol(`a`b`c)").await.unwrap();
    if let Some(ref c) = res {
        println!("创建符号向量: {}", c);
    }

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("获取符号向量: {}", c);
    }
}

// ==================== blob.rs ====================
async fn test_blob() {
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    builder.with_auth(("admin", "123456"));
    let mut client = builder.connect().await.unwrap();

    let mut variables = HashMap::new();

    let res = client
        .run_script("a = blob(`a);\nb = blob(`abc`de);")
        .await
        .unwrap();
    if let Some(ref c) = res {
        println!("创建BLOB: {}", c);
    }

    variables.insert("a".to_string(), Blob::new(vec![b'a']).into());
    variables.insert(
        "b".to_string(),
        Vector::<Blob>::from_raw(&[&vec![b'a'], &vec![b'b']]).into(),
    );

    client.upload(&variables).await.unwrap();

    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("获取BLOB a: {}", c);
    }

    let res = client.run_script("b").await.unwrap();
    if let Some(ref c) = res {
        println!("获取BLOB b: {}", c);
    }
}

// ==================== data_forms.rs ====================
fn test_data_forms() {
    // 创建空数据结构
    let mut v = IntVector::new();
    v.push(1.into());
    
    let mut s = Set::<Int>::new();
    s.insert(1.into());
    
    let mut d = Dictionary::<Int>::new();
    d.insert(1.into(), Int::new(2));
    
    let mut a = IntArrayVector::new();
    a.push(vec![1, 2, 3]);
}

// ==================== 主执行函数 ====================
#[tokio::main]
async fn main() {
    println!("============ 开始类型转换测试 ============");
    test_type_conversion().await;
    
    println!("\n============ 开始表格插入测试 ============");
    test_table_insert().await.unwrap();
    
    println!("\n============ 开始Any向量测试 ============");
    test_any_vector().await;
    
    println!("\n============ 开始订阅测试 ============");
    let t1 = tokio::spawn(async { test_subscribe("example1".into()).await });
    let t2 = tokio::spawn(async { test_subscribe("example2".into()).await });
    t1.await.unwrap();
    t2.await.unwrap();
    
    println!("\n============ 开始快速入门测试 ============");
    test_quick_start().await;
    
    println!("\n============ 开始表格写入测试 ============");
    test_table_writer().await.unwrap();
    
    println!("\n============ 开始符号向量测试 ============");
    test_symbol_vector().await;
    
    println!("\n============ 开始BLOB测试 ============");
    test_blob().await;
    
    println!("\n============ 开始数据结构测试 ============");
    test_data_forms();
}

// ==================== 辅助结构体和函数 ====================
#[derive(Clone)]
struct TickerEvent {
    event_time: i64,
    event_time2: NaiveDateTime,
    symbol: String,
    event_id: i64,
    prices: Vec<f64>,
}

fn build_table_row(event: &TickerEvent) -> Vec<PrimitiveType> {
    vec![
        event.event_time.into(),
        event.event_time2.into(),
        event.symbol.clone().into(),
        event.event_id.into(),
        event.prices.clone().into(),
    ]
}
