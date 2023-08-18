const { Kafka } = require("kafkajs");
const axios = require('axios');

const KITCHEN_BASE_URL = 'http://localhost:3000/api/kitchen/'

function getCurrentISTTime() {
    const date = new Date();
    const offset = date.getTimezoneOffset() == 0 ? 0 : -1 * date.getTimezoneOffset();
    let normalized = new Date(date.getTime() + (offset) * 60000);
    let currentISTTime = new Date(normalized.toLocaleString("en-US", {timeZone: "Asia/Calcutta"}));
    return currentISTTime;
}

run();
async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["127.0.0.1:9092"],
    });

    const consumer = kafka.consumer({ groupId: "testGroup" });
    await consumer.connect();
    console.log("Consumer connected!");
    consumer.subscribe({
      topic: "Orders",
      fromBeginning: true,
    });

    let orderData;
    // consume the order received
    await consumer.run({
      eachMessage: async (result) => {
        console.log("** Inside eachMessage");
        orderData = JSON.parse(result.message.value);
        orderData.kitchen_receiving_time = getCurrentISTTime();
        console.log("Consumed order: ", orderData);
        axios.post(KITCHEN_BASE_URL, orderData, { headers: {'Content-Type': 'application/json'} });
        console.log(`Sent ${orderData.order_origin} order with ID: ${orderData.order_id} to kitchen`);

        // send order to the kitchen via API call
        // try {
        //     await axios.post(KITCHEN_BASE_URL, orderData, { headers: {'Content-Type': 'application/json'} });
        //     console.log(`Sent ${orderData.order_origin} order with ID: ${orderData.order_id} to kitchen`);
        // } catch (postErr) {
        //     console.log("Error in sending order to kitchen: ", postErr)
        // }
      },
    });
  } catch (err) {
    console.log(err);
  } finally {
    // process.exit(1);
  }
}
