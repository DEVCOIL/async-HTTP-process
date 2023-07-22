// M1/index.js
const express = require('express');
const amqp = require('amqplib/callback_api');
const app = express();


app.use(express.json());


// Функция для отправки задания в RabbitMQ и получения ответа
async function processRequest(data, res) {
    const connection = await new Promise((resolve, reject) => {
        amqp.connect('amqp://localhost', (error0, connection) => {
            if (error0) {
                reject(error0);
            } else {
                resolve(connection);
            }
        });
    });

    const channel = await new Promise((resolve, reject) => {
        connection.createChannel((error1, channel) => {
            if (error1) {
                reject(error1);
            } else {
                resolve(channel);
            }
        });
    });

    const queue = 'task_queue';
    const resultQueue = 'result_queue';
    const message = JSON.stringify(data);

    // Очередь для получения результата
    await new Promise((resolve, reject) => {
        channel.assertQueue(resultQueue, {
            durable: true,
        });
        channel.consume(resultQueue, (msg) => {
            const resultData = JSON.parse(msg.content.toString());
            console.log('Обработано микросервисом M2', resultData);

            // Отправить ответ клиенту
            res.status(200).json(resultData);


            setTimeout(() => {
                connection.close();
            }, 500);
        }, {
            noAck: true,
        });

        // Отправить в очередь
        channel.assertQueue(queue, {
            durable: true,
        });
        channel.sendToQueue(queue, Buffer.from(message), {
            persistent: true,
        });
        console.log('Отправлено в очередь', message);
    });
}


app.post('/process', async (req, res) => {
    const data = req.body;
    try {
        // Обработать запрос в m2 через RabbitMQ и получить результат
        await processRequest(data, res); // Передаем res в функцию
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 'error' });
    }
});




const port = 3000;
app.listen(port, () => {
    console.log(`M1 listening at http://localhost:${port}`);
});
