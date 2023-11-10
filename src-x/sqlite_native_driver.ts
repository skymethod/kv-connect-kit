import { Database, DatabaseOpenOptions, Statement } from 'https://deno.land/x/sqlite3@0.9.1/mod.ts';
import { Row, SqliteDb, SqliteDriver, SqlitePreparedStatement, SqliteQueryParam } from './sqlite_driver.ts';

/** Implementation with native libs based on x/sqlite3. */
export class SqliteNativeDriver implements SqliteDriver {
    private readonly options?: DatabaseOpenOptions;

    constructor(options?: DatabaseOpenOptions) {
        this.options = options;
    }

    newDb(path: string): SqliteDb {
        return new SqliteNativeDb(new Database(path, this.options));
    }
    
}

//

class SqliteNativeDb implements SqliteDb {
    private readonly db: Database;

    constructor(db: Database) {
        this.db = db;
    }

    get changes() {
        return this.db.changes;
    }

    transaction<V>(closure: () => V): V {
        const out: V[] = [];
        const tx = this.db.transaction<void>(() => {
            out.push(closure());
        });
        tx();
        return out[0];
    }

    execute(sql: string) {
        this.db.exec(sql);
    }

    query<R extends Row>(sql: string, params?: SqliteQueryParam[]): R[] {
        const statement = this.prepareStatement<R>(sql);
        try {
            return statement.query(params);
        } finally {
            statement.finalize();
        }
    }
    
    prepareStatement<R extends Row>(sql: string): SqlitePreparedStatement<R> {
        return new SqliteNativePreparedStatement<R>(this.db.prepare(sql));
    }

    close() {
        this.db.close();
    }

}

class SqliteNativePreparedStatement<R extends Row> implements SqlitePreparedStatement<R> {
    private readonly statement: Statement;

    constructor(statement: Statement) {
        this.statement = statement;
    }

    query(params: SqliteQueryParam[] = []): R[] {
        return this.statement.values<R>(params);
    }

    finalize() {
        this.statement.finalize();
    }
    
}
