// M2/worker.js
const amqp = require('amqplib/callback_api');
const winston = require('winston');


// Настройка логгера
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  defaultMeta: { service: 'M2-worker' },
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'm2-worker.log' }),
  ],
});




// Подключение к RabbitMQ
amqp.connect('amqp://localhost', (errorСonnect, connection) => {
  if (errorСonnect) {
    logger.error(`Failed to connect to RabbitMQ: ${errorСonnect.message}`);
    throw errorСonnect;
  }
  connection.createChannel((errorСreateChannel, channel) => {
    if (errorСreateChannel) {
      logger.error(`Failed to create RabbitMQ channel:  ${errorСreateChannel.message}`);
      throw errorСreateChannel;
    }

    const queue = 'task_queue';

    // Обработчик заданий из очереди RabbitMQ
    channel.assertQueue(queue, {
      durable: true,
    });
    channel.consume(queue, (message) => {
      const data = JSON.parse(message.content.toString());
      logger.info(`Received task: ${JSON.stringify(data)}`);

      // Здесь происходит обработка задания из RabbitMQ (M2)
      // Например, выполнение какой-либо работы и получение результата

      // Помещаем результат обработки задания в новую очередь для ответа
      const resultQueue = 'result_queue';
      const result = JSON.stringify({ result: `${JSON.stringify(data)} - 'было получено из очереди, обработано и ввозвращено в RabbitMQ.` });
      logger.info(`Processed task, result: ${JSON.stringify(result)}`);

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
