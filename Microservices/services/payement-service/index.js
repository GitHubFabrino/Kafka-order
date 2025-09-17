import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";





const app = express();

app.use(cors({
    origin: "http://localhost:3000"
}));

app.use(express.json());

const kafka = new Kafka({
    clientId: "kafka-service",
    brokers: ["localhost:9094"],
})

const producer = kafka.producer();

const connectToKafka = async () => {
    try {
        await producer.connect();
        console.log("Producer connected to Kafka");
    } catch (error) {
        console.log("Error connecting to Kafka", error);
    }
}



app.use((req, res, next, err) => {
    console.log(err);
    res.status(err?.status || 500).json({ message: err?.message || "Something went wrong!" });
})

app.post("/payment-service", async (req, res) => {

    const { cart } = req.body;

    const userId = '123'
    console.log('API called');

    // To do payment processing 


    // KAFKA

    await producer.send({
        topic: "payment-successful",
        messages: [
            {
                value: JSON.stringify({
                    userId,
                    cart,
                })
            }
        ]
    });


    return res.status(200).send("Payment successful");


})
app.listen(8000, () => {
    connectToKafka();
    console.log("Payement service is running on port 8000");
});
