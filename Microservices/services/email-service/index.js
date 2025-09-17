
import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "email-service",
    brokers: ["localhost:9094"],
})

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "email-service" });
const run = async () => {
    try {
        await producer.connect();
        await consumer.connect();
        await consumer.subscribe({
            topic: "order-successful", 
            fromBeginning: true 
        });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                const { userId, cart , orderId} = JSON.parse(value);

                console.log(`Email consumer: email send to user ${userId} with order ${orderId}`);

                //    TODO: Send email to user

                const dummyEmailId = "3215464"
                await producer.send({
                    topic: "email-successful",
                    messages: [
                        {
                            value: JSON.stringify({
                                userId,
                                cart,
                                emailId: dummyEmailId
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