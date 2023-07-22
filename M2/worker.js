// M2/worker.js
const amqp = require('amqplib/callback_api');

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

    // Обработчик заданий из очереди RabbitMQ
    channel.assertQueue(queue, {
      durable: true,
    });
    channel.consume(queue, (message) => {
      const data = JSON.parse(message.content.toString());
      console.log('Получено из очереди', data);

      // Здесь происходит обработка задания из RabbitMQ (M2)
      // Например, выполнение какой-либо работы и получение результата

      // Помещаем результат обработки задания в новую очередь для ответа
      const resultQueue = 'result_queue';
      const result = JSON.stringify({ result: `${JSON.stringify(data)} - 'было получено из очереди, обработано и ввозвращено в RabbitMQ.` });

      channel.assertQueue(resultQueue, {
        durable: true,
      });
      channel.sendToQueue(resultQueue, Buffer.from(result), {
        persistent: true,
      });

      // Подтверждение обработки сообщения из очереди
      channel.ack(message);
    }, {

      noAck: false,

    });

  });
});
