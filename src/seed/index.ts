import program from 'commander';
import { getClient } from '../client';
import {
  insert,
  createTopic,
  producer,
  createKsqlTable,
  executeKsql,
} from './users';
import { execLogged } from './util';
import * as usersDiff from './users-diff';

program
  .option('-f, --full-seed', 'Perform a full seed for users diff processing')
  .option(
    '-d, --diff-seed',
    'Perform later half of seed after basic user setup for users diff processing',
  )
  .option('-c, --create-topic', 'Create the users topic')
  .option(
    '-i, --insert [number]',
    'Insert provided number of fake users to the topic',
  )
  .option(
    '-p, --producer [sleep milliseconds]',
    'Start a continuous producer that will send a user message every X milliseconds (default of 500)',
  )
  .option('--create-ksql-table', 'Create a ksql table for a users topic')
  .option('-k, --ksql [query]', 'Execute any ksql query')
  .option(
    '-t, --topic [topic]',
    'Specify different topic for either create, insert or producer functionality',
  )
  .parse(process.argv);

export const run = async (): Promise<void> => {
  program.topic = program.topic || 'users';

  try {
    const client = await getClient();

    if (program.fullSeed || program.createTopic)
      await execLogged('createTopic', createTopic(client));

    if (program.fullSeed || program.createKsqlTable) {
      await execLogged(
        'createKsqlTable',
        createKsqlTable(program.topic),
        'Creating ksql table for USERS',
      );
    }

    if (program.fullSeed || program.diffSeed) {
      await execLogged(
        'createTopic',
        usersDiff.createTopic(client),
        'for new users',
      );
      await execLogged(
        'createKsqlTable',
        createKsqlTable('new_users'),
        'for new users',
      );
    }

    if (program.insert) {
      await execLogged(
        'insert',
        insert(client, Number(program.insert), program.topic),
      );
    }

    if (program.ksql) await executeKsql(program.ksql).then(console.log);

    if (program.producer) {
      const sleepMilliseconds =
        program.producer === true ? 500 : program.producer;
      await execLogged(
        'producer',
        producer(client, Number(program.producer), program.topic),
        `Fires every ${sleepMilliseconds} milliseconds. Hit Ctrl-C to exit.`,
      );
    }

    process.exit(0);
  } catch (e) {
    console.log('An error occurred', e);
    process.exit(1);
  }
};

run();
