import * as net from "net";

// Data storage for sorted set commands
const sortedSet: { [key: string]: { [member: string]: number } } = {};

export function handleZadd(connection: net.Socket, args: string[]): void {
    console.error("ZADD command received");

    const setName = args[0];
    const score = Number(args[1]);
    const member = args[2];

    if (!sortedSet[setName]) {
        sortedSet[setName] = {};
    }

    const isNew = !sortedSet[setName][member];
    sortedSet[setName][member] = score;

    connection.write(`:${isNew ? 1 : 0}\r\n`);
}

export function handleZrank(connection: net.Socket, args: string[]): void {
    console.error("ZRANK command received");

    const setName = args[0];
    const member = args[1];

    if (!sortedSet[setName] || !(member in sortedSet[setName])) {
        connection.write(`$-1\r\n`);
    } else {
        // Get all members and their scores, sort by score (asc), then by member (lex)
        // Get array of [member, score] pairs
        const entries = Object.entries(sortedSet[setName]);

        // Sort entries by score, then by member name
        entries.sort((a, b) => {

            if (a[1] === b[1]) {
                // Lexicographical order if scores are equal
                return a[0].localeCompare(b[0]);
            }

            // Ascending order by score
            return a[1] - b[1];
        });

        // Find index of the member
        const idx = entries.findIndex(([m, _]) => m === member);

        connection.write(`:${idx}\r\n`);
    }
}

export function handleZrange(connection: net.Socket, args: string[]): void {
    console.error("ZRANGE command received");

    const setName = args[0];

    if (!sortedSet[setName]) {
        connection.write(`*0\r\n`);
        return;
    }

    const size = Object.keys(sortedSet[setName]).length;

    // Parse start index from args[1], defaulting to 0 if missing or invalid
    let start = parseInt(args[1] ?? "0", 10);
    let end = parseInt(args[2] ?? (size - 1).toString(), 10);

    if (isNaN(start)) start = 0;
    if (isNaN(end)) end = size - 1;

    if (start < 0) start += size;
    if (end < 0) end += size;

    if (start < 0) start = 0;

    if (start >= size || start > end) {
        connection.write(`*0\r\n`);
        return;
    }

    console.log(start);
    console.log(end);

    // sortedSet[setName] is an object, not an array; construct sorted array
    const entries = Object.entries(sortedSet[setName]);
    // Sort by score (asc), then member name (lex)
    entries.sort((a, b) => {
        if (a[1] === b[1]) {
            return a[0].localeCompare(b[0]);
        }
        return a[1] - b[1];
    });

    const members = entries.map(([member, _]) => member);
    const result = members.slice(start, end + 1);

    connection.write(`*${result.length}\r\n`);

    for (const item of result) {
        connection.write(`$${item.length}\r\n${item}\r\n`)
    }
}

export function handleZcard(connection: net.Socket, args: string[]): void {
    console.error(`ZCARD COMMAND RECEIVED`);

    const setName = args[0];
    if (!sortedSet[setName]) {
        connection.write(`:0\r\n`);
    }
    else {
        const size = Object.keys(sortedSet[setName]).length;
        connection.write(`:${size}\r\n`);
    }
}

export function handleZscore(connection: net.Socket, args: string[]): void {
    console.error(`ZSCORE COMMAND RECEIVED`);

    const setName = args[0];
    const member = args[1];

    if (!sortedSet[setName] || !(member in sortedSet[setName])) {
        connection.write(`$-1\r\n`);
    }
    else {
        const score = sortedSet[setName][member];
        const size = score.toString().length;
        connection.write(`$${size}\r\n${score}\r\n`);
    }
}

export function handleZrem(connection: net.Socket, args: string[]): void {
    console.error(`ZREM COMMAND RECEIVED`);

    const setName = args[0];
    const member = args[1];

    if (!sortedSet[setName] || !(member in sortedSet[setName])) {
        connection.write(`:0\r\n`);
    } else {
        delete sortedSet[setName][member];

        if (Object.keys(sortedSet[setName]).length === 0) {
            delete sortedSet[setName];
        }

        connection.write(`:1\r\n`);
    }
}
