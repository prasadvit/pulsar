mod consumer;

use std::ops::Add;
use pulsar::{
    message::{ Payload}, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
     DeserializeMessage,
};

use uuid::Uuid;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
struct AudioData {
    pcm_bytes: Vec<u8>,
    sample_rate: u16,
}
#[derive(Serialize, Deserialize)]
struct Channel {
    channel_id: i8,
    data :  Vec<AudioData>,
}


#[derive(Serialize, Deserialize)]
struct Frame {
    id: i64,
    seq_no: i64,
    channels: Vec<Channel>,
}
#[derive(Serialize, Deserialize)]
struct Admin {
    topic : String,
}

impl SerializeMessage for Frame {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload =
            serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl SerializeMessage for Channel {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload =
            serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl SerializeMessage for AudioData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload =
            serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}


impl SerializeMessage for Admin {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload =
            serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}




impl DeserializeMessage for Frame {

    type Output = Result<Frame, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

impl DeserializeMessage for Admin {

    type Output = Result<Admin, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}



fn main() {
    println!("Running the producer");
    let  admin : String  = String:: from("non-persistent://public/default/admin");
    let  ADMIN : &str = "admin";
    let  messageQueue: String = String::from("non-persistent://public/default/voxflo");

    for  i  in 0..=5 {
        producer(admin.clone(), ADMIN, i);
    }
    for  i  in 0..=5 {

        producer(messageQueue.clone(), &"message", i);
    }

    // let  admin : String  = String:: from("non-persistent://public/default/admin");
    // let  messageQueue: String = String::from("non-persistent://public/default/voxflo");
    // producer(admin, ADMIN, 1);
    // producer(messageQueue, &"message", 25);


    // let  admin : String  = String:: from("non-persistent://public/default/admin");
    // let  messageQueue: String = String::from("non-persistent://public/default/voxflo");
    // producer(admin, ADMIN, 3);
    // producer(messageQueue, &"message", 25);


    println!("finished");
}

#[tokio::main]
async fn producer(mut topic : String, typ : &str, topicNum: i64 ) -> Result<(), pulsar::Error>  {


    let addr = "pulsar://127.0.0.1:6650";
    println!("topic is {}", topic);
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    if ! typ.eq("admin") {
        topic.push_str(&topicNum.to_string());
    }
    println!("topic to which the message is given {}", topic);
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .with_name("my producer")
        .build()
        .await?;


    let mut topicNumMut: i64 = topicNum;
    let mut counter = 25;

    if typ.eq("admin") {
        println!("posting message admin");
        producer
            .send(make_admin_message(topicNumMut))
            .await?;
    } else {
        println!("posting message");
        while counter > 0 {
            producer
                .send(makeFrames(counter))
                .await?;

            counter -= 1;
            println!("{} messages", counter);

            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        }
    }

    Ok(())
}

    fn make_admin_message(topicNumber : i64) -> Admin {

       let mut  topicStr :String  = String::from("non-persistent://public/default/voxflo");
        topicStr.push_str( &topicNumber.to_string());
        println!("making message ququ {}", topicStr);
        let adminMessage : Admin = Admin {
            topic : topicStr.clone(),
        };
        adminMessage
    }

    fn makeFrames(seqNo:i64 ) -> Frame {
        println!("making frema ");
        let audioData : AudioData = AudioData {
            pcm_bytes : vec![1,0,1,0],
            sample_rate : 16000,
        };

        let channel : Channel = Channel {
            channel_id: 1,
            data: vec![audioData],
        };

        let frame :Frame = Frame {
            id : 0,

            seq_no: seqNo,
            channels: vec![channel],
        };

        frame
    }

