import * as fs from "fs";
import { getSetMap } from "../commands/basics";
const setMap = getSetMap();

/**
 * Implementation theory (detailed steps):
 * 1. Read the RDB file completely into a Buffer for byte-level parsing.
 * 2. Validate the "REDIS" magic header to confirm a valid RDB format.
 * 3. Traverse byte-by-byte to identify RDB sections:
 *      - 0xFA → metadata / auxiliary info
 *      - 0xFE → database selector
 *      - 0xFB → key-value table marker
 *      - 0xFF → end of file
 * 4. Handle expiry markers before keys:
 *      - 0xFC → expiry in milliseconds (8 bytes little-endian)
 *      - 0xFD → expiry in seconds (4 bytes little-endian)
 * 5. Decode keys and values using Redis length-encoding scheme.
 * 6. Return structured list of { key, value, expiry? }.
 */

type Pair = { key: string; value: string; expiry?: number };

/**
 * Decode size encoding used in RDB for strings and lengths.
 * Supports:
 *  - 00xxxxxx : 6-bit length
 *  - 01xxxxxx <next byte> : 14-bit length
 *  - 10xxxxxx : next 4 bytes (big-endian)
 *  - 11xxxxxx : special encodings (0xC0–0xFF)
 */
function decodeSizeEncoding(buffer: Buffer, index: number) {
    if (index >= buffer.length) return { size: 0, endIndex: index };

    const first = buffer[index];
    const top = first >> 6;

    if (top === 0) {
        const size = first & 0x3f;
        return { size, endIndex: index };
    } 
    else if (top === 1) {
        if (index + 1 >= buffer.length) return { size: 0, endIndex: index };
        const size = ((first & 0x3f) << 8) | buffer[index + 1];
        return { size, endIndex: index + 1 };
    } 
    else if (top === 2) {
        if (index + 4 >= buffer.length) return { size: 0, endIndex: index };
        const size = buffer.readUInt32BE(index + 1);
        return { size, endIndex: index + 4 };
    } 
    else {
        return { size: first, endIndex: index };
    }
}

/**
 * Decode a Redis-encoded string starting at a given index.
 * Handles:
 *  - Standard strings (length-encoded)
 *  - Immediate integers (0xC0–0xC2)
 *  - Skips unsupported compression (0xC3)
 */
function decodeStringEncoding(buffer: Buffer, index: number) {
    const { size, endIndex } = decodeSizeEncoding(buffer, index);

    if (size === 0xc0) {
        const val = buffer[index + 1].toString();
        return { encodedString: val, endIndex: index + 1 };
    } 
    else if (size === 0xc1) {
        const val = buffer.readUInt16LE(index + 1).toString();
        return { encodedString: val, endIndex: index + 2 };
    } 
    else if (size === 0xc2) {
        const val = buffer.readUInt32LE(index + 1).toString();
        return { encodedString: val, endIndex: index + 4 };
    } 
    else if (size === 0xc3) {
        throw new Error("LZF-compressed strings not supported");
    } 
    else {
        const headerLen = endIndex - index + 1;
        const start = index + headerLen;
        const end = start + (size as number);
        if (end > buffer.length) throw new Error("Truncated string in RDB");
        const encodedString = buffer.subarray(start, end).toString("utf8");
        return { encodedString, endIndex: end - 1 };
    }
}

/**
 * Main RDB extractor (buffer-based robust parser)
 * Reads RDB file byte-by-byte to accurately extract key-value pairs and expiries.
 */
function extractKeysFromRDB(filePath: string): Pair[] {
    const pairs: Pair[] = [];

    try {
        const file = fs.readFileSync(filePath);
        if (file.length < 9) return pairs;

        const magic = file.subarray(0, 5).toString("utf8");
        if (magic !== "REDIS") return pairs;

        const sectionMarkers = [0xfa, 0xfe, 0xfb, 0xff];
        let i = 8;

        while (i < file.length) {
            const byte = file[i];

            // 0xFA - metadata section
            if (byte === 0xfa) {
                i++;
                while (i < file.length && !sectionMarkers.includes(file[i])) {
                    try {
                        const { endIndex } = decodeStringEncoding(file, i);
                        i = endIndex + 1;
                    } catch {
                        i++;
                    }
                }
                continue;
            }

            // 0xFE - database section start
            else if (byte === 0xfe) {
                const dbInfo = decodeSizeEncoding(file, i + 1);
                i = dbInfo.endIndex;

                // Expect 0xFB marker (hash table start)
                if (file[i + 1] === 0xfb) i++;
                else { i++; continue; }

                const kvSizeInfo = decodeSizeEncoding(file, i + 1);
                i = kvSizeInfo.endIndex;
                const expiryInfo = decodeSizeEncoding(file, i + 1);
                i = expiryInfo.endIndex;

                const tableSize = kvSizeInfo.size ?? 0;
                let count = 0;

                // Iterate through key-value pairs
                while (count < tableSize && i < file.length) {
                    let expiry: number | null = null;

                    // Check expiry markers
                    if (file[i + 1] === 0xfc) {
                        i++;
                        if (i + 8 <= file.length - 1) {
                            expiry = Number(file.readBigUInt64LE(i + 1));
                            i += 8;
                        } else break;
                    } 
                    else if (file[i + 1] === 0xfd) {
                        i++;
                        if (i + 4 <= file.length - 1) {
                            expiry = file.readUInt32LE(i + 1) * 1000;
                            i += 4;
                        } else break;
                    }

                    // Value type (ignored in this context)
                    i++;
                    const valueType = file[i];

                    try {
                        // Decode key and value sequentially
                        const { encodedString: key, endIndex: endKey } = decodeStringEncoding(file, i + 1);
                        i = endKey;
                        const { encodedString: value, endIndex: endVal } = decodeStringEncoding(file, i + 1);
                        i = endVal;

                        if (key && value) {
                            pairs.push({
                                key,
                                value,
                                expiry: expiry ?? undefined
                            });
                            count++;
                        } else break;
                    } 
                    catch {
                        break;
                    }
                }
            }

            // 0xFF - end of file (EOF marker)
            else if (byte === 0xff) {
                i += 9; // skip checksum bytes
                break;
            }

            // Skip unknown byte
            else i++;
        }
    } 
    catch (error) {
        return pairs;
    }

    return pairs;
}

// Theory of the implementation (concise points): 
// 1. Fallback strategy for extracting a Redis key from a possibly arbitrary RDB file: 
// 2. Loads the entire file into a buffer for byte-wise access. 
// 3. Iterates through every byte, collecting sequences of printable ASCII characters (32 to 126 inclusive). 
// 4. Groups each contiguous sequence of printable characters as a 'word'. 
// 5. After extraction, filters out likely non-key words (e.g., metadata, config params, .rdb filenames) by regex and suffix tests. 
// 6. Among the remaining words, returns the first that is at least 3 characters long and contains at least one ASCII letter. 
// 7. If none pass the filter, returns null.
function fallbackExtractKeys(filePath: string): Pair[] {
    const pairs: Pair[] = [];
    try {
        const file = fs.readFileSync(filePath);

        let curr = "";
        const words: string[] = [];

        for (const byte of file) {
            if (byte >= 32 && byte <= 126) curr += String.fromCharCode(byte);
            else if (curr.length) {
                words.push(curr);
                curr = "";
            }
        }

        if (curr.length) words.push(curr);

        const skip = [
            /^REDIS\d+$/i,
            /^redis[-,_]/i,
            /^dbfilename&/i,
            /^dir&/i
        ];

        const clean = words.filter(
            (w) => !skip.some((s) => s.test(w)) && w.length >= 2 && /[A-Za-z]/.test(w)
        );

        for (let i = 0; i < clean.length - 1; i += 2) {
            pairs.push({ key: clean[i], value: clean[i + 1] });
        }

        return pairs;
    } 
    catch (error) {
        return [];
    }
}

/**
 * Public entry point: loads and populates data from RDB.
 *  - Reads file path from CLI args (--dir and --dbfilename)
 *  - Parses via buffer-based reader
 *  - Falls back to ASCII parser if needed
 *  - Skips expired keys
 */
export function RDBsetup(): void {
    const argv = process.argv;
    const dirIdx = argv.indexOf("--dir");
    const dbIdx = argv.indexOf("--dbfilename");

    if (dirIdx !== -1 && dbIdx !== -1 && argv[dirIdx + 1] && argv[dbIdx + 1]) {
        const dbFilePath = `${argv[dirIdx + 1]}/${argv[dbIdx + 1]}`;

        if (fs.existsSync(dbFilePath)) {
            let pairs = extractKeysFromRDB(dbFilePath);
            if (!pairs.length) pairs = fallbackExtractKeys(dbFilePath);

            const now = Date.now();

            for (const item of pairs) {
                if (typeof item.expiry === "number" && item.expiry <= now) continue;
                setMap[item.key] = {
                    value: item.value,
                    expiry: item.expiry
                };
            }
        }
    }
}
