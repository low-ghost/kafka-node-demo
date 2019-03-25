/**
 * User topic creation and seeding utils.
 */
import { KafkaClient, CreateTopicResponse } from 'kafka-node';

export const createTopic = (
  client: KafkaClient,
): Promise<CreateTopicResponse[]> => {
  const config = {
    topic: 'new_users',
    partitions: 1,
    replicationFactor: 1,
  };
  return new Promise((resolve, reject) =>
    client.createTopics([config], (err, res) =>
      err ? reject(err) : resolve(res),
    ),
  );
};
