
import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "order-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
})

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });
const run = async () => {
   try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: "payment-successful", fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
           const value = message.value.toString();
           const {userId, cart} = JSON.parse(value);

           console.log(`Order consumer: order created for user ${userId}`);

        //    TODO: Create order on database

        const dummyOrderId = "124578746"
        await producer.send({
            topic: "order-successful",
            messages: [
                {
                    value: JSON.stringify({
                        userId,
                        cart,
                        orderId: dummyOrderId
                    })
                }
            ]
        })

        //    TODO: Send email to user

        //    TODO: Send order to analytics service

        //    TODO: Send order to email service

         
        },
    });
   } catch (error) {
    console.log(error);
   }
}
run();