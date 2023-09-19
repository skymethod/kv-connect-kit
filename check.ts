
export function isRecord(obj: unknown): obj is Record<string, unknown> {
    return typeof obj === 'object' && obj !== null && !Array.isArray(obj) && obj.constructor === Object;
}
