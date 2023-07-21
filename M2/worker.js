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
      console.log(' [x] Received %s', data);

      // Здесь происходит обработка задания из RabbitMQ (M2)
      // Например, выполнение какой-либо работы и получение результата

      // Помещаем результат обработки задания в новую очередь для ответа
      const resultQueue = 'result_queue';
      const result = JSON.stringify({ processedData: 'some_result' });

      channel.assertQueue(resultQueue, {
        durable: true,
      });
      channel.sendToQueue(resultQueue, Buffer.from(result), {
        persistent: true,
      });

      // Подтверждение обработки сообщения из очереди
      channel.ack(message);
    }, {
      // Одинаковые задания не будут отправляться одновременно на разные worker'ы
      noAck: false,
    });
  });
});
