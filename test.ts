import net from "net";
import readline from "readline";

// Connect to your Redis clone
const client = net.createConnection({ port: 6379, host: "127.0.0.1" }, () => {
  console.log("Connected to Redis clone at 127.0.0.1:6379");
});

// Setup readline for interactive input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: "redis> "
});

// Helper: convert command string to RESP
function toRESP(command: string): string {
  const parts = command.trim().split(/\s+/);
  let resp = `*${parts.length}\r\n`;
  for (const part of parts) {
    resp += `$${Buffer.byteLength(part, "utf-8")}\r\n${part}\r\n`;
  }
  return resp;
}

let inTransaction = false;  // Track MULTI/EXEC state
let queuedCommands: string[] = [];

rl.prompt();

rl.on("line", (line: string) => {
  const trimmed = line.trim();
  if (!trimmed) {
    rl.prompt();
    return;
  }

  // Detect MULTI start
  if (trimmed.toUpperCase() === "MULTI") {
    inTransaction = true;
    queuedCommands = [];
  }

  if (inTransaction && trimmed.toUpperCase() !== "EXEC" && trimmed.toUpperCase() !== "DISCARD") {
    queuedCommands.push(trimmed);
  }

  const respCommand = toRESP(trimmed);
  client.write(respCommand);

  // Show queued commands
  if (inTransaction && trimmed.toUpperCase() !== "EXEC" && trimmed.toUpperCase() !== "DISCARD") {
    console.log(`(queued) ${trimmed}`);
  }

  rl.prompt();
}).on("close", () => {
  console.log("\nExiting.");
  client.end();
  process.exit(0);
});

// Handle server responses
client.on("data", (data: Buffer) => {
  const response = data.toString();

  if (response.startsWith("*")) {
    // Multi-bulk reply (e.g., EXEC results)
    const lines = response.split("\r\n").filter(Boolean);
    console.log("(transaction results)");
    lines.forEach((line, idx) => {
      console.log(`${idx + 1}) ${line}`);
    });
    inTransaction = false;
    queuedCommands = [];
  } else {
    process.stdout.write(response);
  }

  rl.prompt();
});

client.on("end", () => {
  console.log("\nDisconnected from Redis clone.");
});
