
import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "analytic-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
})

const consumer = kafka.consumer({ groupId: "analytic-service" });
const run = async () => {
    try {
        await consumer.connect();

        await consumer.subscribe({
            topics: ["payment-successful", "order-successful", "email-successful"],
            fromBeginning: true
        });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {

                try {
                    const value = message.value.toString();
                    const data = JSON.parse(value);
                    
                    switch (topic) {
                        case "payment-successful":
                            if (!data.cart || !Array.isArray(data.cart)) {
                                console.error('Invalid cart data in payment message:', data);
                                break;
                            }
                            const totalPayment = data.cart.reduce((acc, item) => acc + (item.price || 0), 0).toFixed(2);
                            console.log(`Analytic consumer: user ${data.userId || 'unknown'} made a purchase of ${totalPayment}`);
                            break;
                    
                        case "order-successful":
                            console.log(`Analytic consumer: order created id ${data.orderId || 'unknown'} for user ${data.userId || 'unknown'}`);
                            break;

                        case "email-successful":
                            console.log(`Analytic consumer: email sent id ${data.emailId || 'unknown'} to user ${data.userId || 'unknown'}`);
                            break;

                        default:
                            console.log(`Received message on unhandled topic: ${topic}`);
                            break;
                    }
                } catch (error) {
                    console.error(`Error processing message from topic ${topic}:`, error);
                }

            },
        });
    } catch (error) {
        console.log(error);
    }
}
run();