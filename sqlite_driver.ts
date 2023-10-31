// minimal functionality needed by a sqlite implementation

export interface SqliteDriver {
    newDb(path: string): SqliteDb;
}

export interface SqliteDb {
    readonly changes: number;
    transaction<V>(closure: () => V): V;
    execute(sql: string): void;
    query<R extends Row>(sql: string, params?: SqliteQueryParam[]): R[];
    prepareStatement<R extends Row>(sql: string): SqlitePreparedStatement<R>;
    close(): void;
}

export interface SqlitePreparedStatement<R extends Row> {
    query(params?: SqliteQueryParam[]): R[];
    finalize(): void;
}

export type Row = unknown[];

export type SqliteQueryParam =
    | null
    | number
    | string
    | Uint8Array;
