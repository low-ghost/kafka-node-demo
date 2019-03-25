import 'isomorphic-fetch';
import { KafkaClient } from 'kafka-node';

const KSQL_URL = 'http://localhost:8088';

export const getClient = async (): Promise<KafkaClient> => {
  // Get client for local kafka.
  const client = new KafkaClient({ kafkaHost: 'localhost:9092' });
  // Wait for ready state.
  await new Promise(resolve => client.on('ready', () => resolve()));
  return client;
};

export const formatKsql = (query: string): string =>
  query.split('\n').join(' ') + ';';

export const formatKsqlBody = (query: string, streamsProperties?: {} | null) =>
  JSON.stringify({
    ksql: formatKsql(query),
    ...(streamsProperties ? { streamsProperties } : {}),
  });

export const getKsqlError = async (response: any) => {
  return new Error(JSON.stringify(await response.json()));
};

export const ksqlStatement = async (
  ...args: [string, {} | null]
): Promise<any> => {
  const headers = {
    'Content-Type': 'application/vnd.ksql.v1+json;charset=UTF-8',
  };
  const params = { method: 'POST', headers, body: formatKsqlBody(...args) };
  const response = await fetch(`${KSQL_URL}/ksql`, params);
  if (!response.ok) throw await getKsqlError(response);
  return await response.json();
};

export const ksqlQuery = async (...args: [string, {} | null]): Promise<any> => {
  const headers = { 'Content-Type': 'application/json' };
  const params = { method: 'POST', headers, body: formatKsqlBody(...args) };
  const response = await fetch(`${KSQL_URL}/query`, params);
  if (!response.ok) throw await getKsqlError(response);

  const buffer = await (response as any).buffer();
  return buffer
    .toString('utf8')
    .split('\n')
    .reduce((acc: any, line: any) => {
      if (line.length < 1) return acc;
      const parsed = JSON.parse(line);
      if (parsed.finalMessage) return acc;
      return parsed.row.columns;
    }, []);
};
