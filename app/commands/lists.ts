import * as net from "net";

// Data storage for list commands
const list: { [key: string]: string[] } = {};
const pending: { [key: string]: net.Socket[] } = {};

export function handleRpush(connection: net.Socket, args: string[]): void {
    console.error("RPUSH command received");
    // RPUSH: Append one or multiple values to a list (right side)
    // If the list does not exist, create it.
    const listName = args[0];
    if (!list[listName]) {
        list[listName] = [];
    }

    // Push all provided elements to the list
    for (let i = 1; i < args.length; i++) {
        list[listName].push(args[i]);
    }

    const size = list[listName].length;

    // If there are pending clients waiting for elements on this list,
    // serve them now by shifting both the client and the element.
    // NOTE: This logic assumes pending clients are waiting for a blocking pop (e.g., BLPOP/BRPOP).
    // TODO: Revisit for edge cases and concurrency issues.
    if (pending[listName] && pending[listName].length) {
        while (pending[listName].length && list[listName].length) {
            const client = pending[listName].shift()!;
            const ele = list[listName].shift()!;
            // Respond to the client with the list name and the element
            client.write(`*2\r\n$${listName.length}\r\n${listName}\r\n$${ele.length}\r\n${ele}\r\n`);
        }
    }

    // Respond to the RPUSH caller with the new size of the list
    connection.write(`:${size}\r\n`);

    // TODO: Consider atomicity and error handling for production use
}

export function handleLrange(connection: net.Socket, args: string[]): void {
    console.error("LRANGE command received");
    const listName = args[0];

    if (!list[listName]) {
        connection.write(`*0\r\n`);
        return;
    }

    const size = list[listName].length;

    let start = Number(args[1]), end = Math.min(Number(args[2]), size - 1);

    if (start < 0) start += size;
    if (end < 0) end += size;

    if (start < 0) start = 0;

    if (start >= size || start > end) {
        connection.write(`*0\r\n`);
        return;
    }

    const result = list[listName].slice(start, end + 1);

    connection.write(`*${result.length}\r\n`);

    for (const item of result) {
        connection.write(`$${item.length}\r\n${item}\r\n`);
    }
}

export function handleLpush(connection: net.Socket, args: string[]): void {
    console.error("LPUSH command received");
    const listName = args[0];
    if (!list[listName]) {
        list[listName] = [];
    }
    let i = 1;
    while (args[i]) {
        list[listName].push(args[i]);
        i++;
    }
    const size = list[listName].length;

    list[listName].reverse();
    connection.write(`:${size}\r\n`);
}

export function handleLlen(connection: net.Socket, args: string[]): void {
    console.error("LLEN command received");
    const listName = args[0];
    if (!list[listName]) {
        connection.write(`:0\r\n`);
        return;
    }
    const size = list[listName].length;

    connection.write(`:${size}\r\n`);
}

export function handleLpop(connection: net.Socket, args: string[]): void {
    console.error("LPOP command received");
    const listName = args[0];
    if (!list[listName]) {
        connection.write(`$-1\r\n`);
        return;
    }

    let stop = 1;
    if (args[1]) {
        stop = Number(args[1]);
    }

    const removed: string[] = list[listName].splice(0, stop);
    const size = removed.length;

    if (stop > 1) {
        connection.write(`*${size}\r\n`);
        for (const item of removed) {
            connection.write(`$${item.length}\r\n${item}\r\n`);
        }
    } else {
        const item = removed[0];
        connection.write(`$${item.length}\r\n${item}\r\n`);
    }
}

export function handleBlpop(connection: net.Socket, args: string[]): void {
    console.error("BLPOP command received");
    // BLPOP: Blocking list pop. If the list is empty, block the connection until an element is available or timeout.
    // TODO: Review if this implementation handles multiple simultaneous BLPOPs on the same list correctly.
    // TODO: Consider edge cases where the list is deleted or modified by other commands while blocked.
    // TODO: Check if pending connections are cleaned up properly on client disconnect.
    // TODO: Evaluate if we should support multiple list names as in Redis (currently only supports one).
    // TODO: Confirm if time=0 should block forever (currently, 0 means no timeout).

    const listName = args[0];
    const time = Number(args[1]) || 0;

    if (list[listName] && list[listName].length) {
        // List has elements, pop and return immediately
        const ele = list[listName].shift()!;
        connection.write(`*2\r\n$${listName.length}\r\n${listName}\r\n$${ele.length}\r\n${ele}\r\n`);
    }
    else {
        // List is empty, add connection to pending
        if (!pending[listName]) pending[listName] = [];
        pending[listName].push(connection);

        // Only set timeout if time > 0 (0 means block forever)
        if (time > 0) {
            setTimeout(() => {
                const idx = pending[listName].indexOf(connection);
                if (idx !== -1) {
                    pending[listName].splice(idx, 1);
                    connection.write(`*-1\r\n`);
                }
            }, time * 1000);
        }
    }
}

export function getPending(): { [key: string]: net.Socket[] } {
    return pending;
}

export function getList(): { [key: string]: string[] } {
    return list;
}
