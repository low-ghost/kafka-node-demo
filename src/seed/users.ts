/**
 * User topic creation and seeding utils.
 */
import casual from 'casual';
import {
  KafkaClient,
  CreateTopicResponse,
  KeyedMessage,
  Producer,
} from 'kafka-node';
import { ksqlStatement, ksqlQuery } from '../client';

export const createTopic = (
  client: KafkaClient,
): Promise<CreateTopicResponse[]> => {
  const config = {
    topic: 'users',
    partitions: 1,
    replicationFactor: 1,
    configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
  };
  return new Promise((resolve, reject) =>
    client.createTopics([config], (err, res) =>
      err ? reject(err) : resolve(res),
    ),
  );
};

type User = {
  id: number;
  email: string;
  name: string;
  phone: string;
};

// Define a custom casual object for random user generation.
casual.define(
  'user',
  (): User => ({
    // So we get collisions that function as metadata changes
    id: casual.integer(1, 20),
    email: casual.email,
    name: casual.first_name,
    phone: casual.phone,
  }),
);

// Infinite sequence of random users.
export function* userSequence() {
  while (true) {
    yield (casual as any).user;
  }
}

// Messages are keyed by the user id and are stringified json.
export const getKeyedMessage = (user: User): KeyedMessage =>
  new KeyedMessage(String(user.id), JSON.stringify(user));

export const insert = (
  client: KafkaClient,
  num: number,
  topic: string,
): Promise<any> => {
  const getUser = userSequence();
  // Hacky do action n times, where action is create a user via the sequence.
  const users = Array(num)
    .fill(1)
    .map(() => getUser.next().value);
  console.log('Inserting users', users);
  const messages = users.map(getKeyedMessage);
  const producer = new Producer(client);
  return new Promise((resolve, reject) =>
    producer.send([{ topic, messages }], (err, res) =>
      err ? reject(err) : resolve(res),
    ),
  );
};

export const producer = async (
  client: KafkaClient,
  sleepMilliseconds: number,
  topic: string,
): Promise<void> => {
  const producer = new Producer(client);
  for (const user of userSequence()) {
    await new Promise(resolve => setTimeout(resolve, sleepMilliseconds));

    try {
      const message = getKeyedMessage(user);
      await new Promise((resolve, reject) =>
        producer.send([{ topic, messages: [message] }], (err, res) =>
          err ? reject(err) : resolve(res),
        ),
      );
      console.log('Successfully inserted user:', user);
    } catch (e) {
      console.log('Failed to insert a user', e);
    }
  }
};

export const createKsqlTable = (topic: string): Promise<any> =>
  ksqlStatement(
    `CREATE TABLE ${topic} (
      id BIGINT,
      name VARCHAR,
      phone VARCHAR,
      email VARCHAR
    ) WITH (
      KAFKA_TOPIC = '${topic}',
      VALUE_FORMAT = 'JSON',
      KEY = 'id'
    )`,
    null,
  );

export const executeKsql = (query: string): Promise<any> =>
  ksqlQuery(query, { 'auto.offset.reset': 'earliest' });
