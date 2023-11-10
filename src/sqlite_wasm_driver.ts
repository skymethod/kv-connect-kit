import { DB, PreparedQuery, SqliteOptions } from 'https://deno.land/x/sqlite@v3.8/mod.ts';
import { Row, SqliteDb, SqliteDriver, SqlitePreparedStatement, SqliteQueryParam } from './sqlite_driver.ts';

/** Cross-platform wasm implementation based on x/sqlite. */
export class SqliteWasmDriver implements SqliteDriver {
    private readonly options?: SqliteOptions;

    constructor(options?: SqliteOptions) {
        this.options = options;
    }

    newDb(path: string): SqliteDb {
        return new SqliteWasmDb(new DB(path, this.options));
    }
    
}

//

class SqliteWasmDb implements SqliteDb {
    private readonly db: DB;

    constructor(db: DB) {
        this.db = db;
    }

    get changes() {
        return this.db.changes;
    }

    transaction<V>(closure: () => V): V {
        return this.db.transaction(closure);
    }

    execute(sql: string) {
        this.db.execute(sql);
    }

    query<R extends Row>(sql: string, params?: SqliteQueryParam[]): R[] {
        return this.db.query<R>(sql, params);
    }
    
    prepareStatement<R extends Row>(sql: string): SqlitePreparedStatement<R> {
        return new SqliteWasmPreparedStatement<R>(this.db.prepareQuery<R>(sql));
    }

    close() {
        this.db.close();
    }

}

class SqliteWasmPreparedStatement<R extends Row> implements SqlitePreparedStatement<R> {
    private readonly pq: PreparedQuery<R>;

    constructor(pq: PreparedQuery<R>) {
        this.pq = pq;
    }

    query(params?: SqliteQueryParam[]): R[] {
        return this.pq.all(params);
    }

    finalize() {
      this.pq.finalize();
    }
    
}
