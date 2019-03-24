import { Consumer } from 'kafka-node';
import { getClient } from '../client';

const run = async () => {
  console.log('Starting a simple log consumer. Hit Ctrl-C to exit');
  const client = await getClient();
  const consumer = new Consumer(
    client,
    [{ topic: 'users', partition: 0 }],
    { autoCommit: false }
  );

  consumer.on('message', (user) => {
    console.log('Received user message', user);
  });
};

run();
