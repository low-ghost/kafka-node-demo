/**
 * User topic creation and seeding utils.
 */
import program from 'commander';
import casual from 'casual';
import {
  KafkaClient,
  CreateTopicRequest,
  CreateTopicResponse,
  KeyedMessage,
  Producer,
} from 'kafka-node';
import { getClient, ksqlStatement, ksqlQuery } from '../client';

program.option('-c, --create-topic', 'Create the users topic');
export const createTopic = (
  client: KafkaClient,
): Promise<CreateTopicResponse[]> => {
  const config = {
    topic: 'users',
    partitions: 1,
    replicationFactor: 1,
    // configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
  };
  return new Promise((resolve, reject) => client.createTopics(
    [config],
    (err, res) => err ? reject(err) : resolve(res),
  ));
};

type User = {
  id: number,
  email: string,
  name: string,
  phone: string,
};

// Define a custom casual object for random user generation.
casual.define('user', (): User => ({
  // So we get collisions that function as metadata changes
  id: casual.integer(1, 20),
  email: casual.email,
  name: casual.first_name,
  phone: casual.phone,
}));

// Infinite sequence of random users.
function *userSequence() {
  while (true) {
    yield (casual as any).user;
  }
}

// Messages are keyed by the user id and are stringified json.
export const getKeyedMessage = (user: User): KeyedMessage =>
  new KeyedMessage(String(user.id), JSON.stringify(user));

program.option(
  '-i, --insert [number]',
  'Insert provided number of fake users to the topic'
);
export const insert = (client: KafkaClient, num: number): Promise<any>  => {
  const getUser = userSequence();
  // Hacky do action n times, where action is create a user via the sequence.
  const users = Array(num).fill(1).map(() => getUser.next().value);
  console.log('Inserting users', users);
  const messages = users.map(getKeyedMessage);
  const producer = new Producer(client);
  return new Promise((resolve, reject) => producer.send(
    [{ topic: 'users', messages }],
    (err, res) => err ? reject(err) : resolve(res)
  ));
};

program.option(
  '-p, --producer [sleep milliseconds]',
  'Start a continuous producer that will send a user message every X milliseconds (default of 500)',
);
export const producer = async (client: KafkaClient, sleepMilliseconds: number) => {
  const producer = new Producer(client);
  for (const user of userSequence()) {
    await new Promise(resolve => setTimeout(resolve, sleepMilliseconds));

    try {
      const message = getKeyedMessage(user);
      await new Promise((resolve, reject) => producer.send(
        [{ topic: 'users', messages: [message] }],
        (err, res) => err ? reject(err) : resolve(res)
      ));
      console.log('Successfully inserted user:', user);
    } catch(e) {
      console.log('Failed to insert a user', e);
    }
  }
};

program.option(
  '--create-ksql-table',
  'Create a ksql table for the users topic',
);
export const createKsqlTable = () => ksqlStatement(`
  CREATE TABLE users (
    id BIGINT,
    name VARCHAR,
    phone VARCHAR,
    email VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'users',
    VALUE_FORMAT = 'JSON',
    KEY = 'id'
  )
`);

program.option(
  '-k, --ksql [query]',
  'Execute any ksql statement',
)
export const executeKsql = (query: string): Promise<any> =>
  ksqlQuery(query, { 'auto.offset.reset': 'earliest' });

// , { 'ksql.streams.auto.offset.reset': 'earliest' }

export const execLogged = async (name: string, promise: Promise<any>) => {
  console.log(`Starting ${name}`);
  const res = await promise;
  console.log(`Done ${name} with results`, res);
};

export const run = async () => {
  try {
    program.parse(process.argv);
    const client = await getClient();

    if (program.createTopic)
      await execLogged('createTopic', createTopic(client));

    if (program.insert)
      await execLogged('insert', insert(client, Number(program.insert)));

    if (program.producer) {
      const sleepMilliseconds = program.producer === true
        ? 500
        : program.producer;
      console.log(
        `Starting producer firing every ${sleepMilliseconds} milliseconds.` +
          ` Hit Ctrl-C to exit.`
      );
      await producer(client, Number(program.producer));
    }

    if (program.createKsqlTable)
      await execLogged('Creating ksql table for USERS', createKsqlTable());

    if (program.ksql) await executeKsql(program.ksql).then(console.log);

    process.exit(0);
  } catch (e) {
    console.log('An error occurred', e);
    process.exit(1);
  }
};

run();
