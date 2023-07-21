// M1/index.js
const express = require('express');
const app = express();
const amqp = require('amqplib/callback_api');

// Middleware для парсинга JSON
app.use(express.json());

// Обработчик HTTP запросов
app.post('/process', async (req, res) => {
    const data = req.body;

    // Подключение к RabbitMQ
    amqp.connect('amqp://localhost', (error0, connection) => {
        if (error0) {
            throw error0;
        }
        connection.createChannel((error1, channel) => {
            if (error1) {
                throw error1;
            }

            const queue = 'task_queue';
            const message = JSON.stringify(data);

            // Опубликовать задание в очереди RabbitMQ
            channel.assertQueue(queue, {
                durable: true,
            });
            channel.sendToQueue(queue, Buffer.from(message), {
                persistent: true,
            });
            console.log(' [x] Sent %s', message);

            // Закрыть соединение с RabbitMQ
            setTimeout(() => {
                connection.close();
            }, 500);
        });
    });

    res.status(202).json({ status: 'accepted' });
});

// Запуск сервера
const port = 3000;
app.listen(port, () => {
    console.log(`M1 listening at http://localhost:${port}`);
});
