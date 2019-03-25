export const execLogged = async (
  name: string,
  promise: Promise<any>,
  additional?: string,
): Promise<void> => {
  console.log(`Starting ${name}`);
  if (additional) console.log(additional);
  const res = await promise;
  console.log(`Done ${name} with results`, res);
};
