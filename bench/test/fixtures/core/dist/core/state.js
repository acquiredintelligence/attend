export const records = [];
export function fromRecord(record, policy) {
  records.push({ record, policy });
  return { scope: record.scope };
}
