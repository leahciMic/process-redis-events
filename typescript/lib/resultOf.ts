/**
 * Safely unwrap the result of a promise, returning a tuple of [error, result].
 * @param promise The promise to unwrap.
 * @returns A tuple containing either [null, result] or [error, null].
 */
export async function resultOf<T>(
  promise: Promise<T> | (() => T)
): Promise<[null, T] | [Error, null]> {
  try {
    return [null, typeof promise === 'function' ? promise() : await promise]
  } catch (error) {
    return [error as Error, null]
  }
}
