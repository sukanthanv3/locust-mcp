import csv
import time
import asyncio
import os
import signal
import uuid
import httpx
from typing import Optional, Dict, Any, List, AsyncGenerator
from fastmcp import FastMCP, Context

mcp = FastMCP("locust-mcp")

# In-memory registry to track active load test processes
ACTIVE_RUNS: Dict[str, asyncio.subprocess.Process] = {}

async def parse_sse_stream(
    byte_stream: AsyncGenerator[bytes, None]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Parses a raw byte stream into Server-Sent Events (SSE) according to the spec:
    Events are separated by double newlines (`\\n\\n`), lines follow `field: value`.
    """
    buffer = ""
    current_event: Dict[str, Any] = {"event": "message", "data": "", "id": None}

    async for chunk in byte_stream:
        buffer += chunk.decode("utf-8", errors="replace")

        # Process complete lines from the buffer
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            line = line.rstrip("\r")

            # An empty line signals the end of an event block
            if not line:
                if current_event["data"]:
                    # Strip trailing newline appended during multi-line data accumulation
                    current_event["data"] = current_event["data"].rstrip("\n")
                    yield dict(current_event)
                # Reset for the next event
                current_event = {"event": "message", "data": "", "id": None}
                continue

            # Ignore comments
            if line.startswith(":"):
                continue

            # Parse "field: value" or bare "field"
            if ":" in line:
                field, value = line.split(":", 1)
                value = value.lstrip(" ")
            else:
                field, value = line, ""

            if field == "data":
                current_event["data"] += value + "\n"
            elif field == "event":
                current_event["event"] = value
            elif field == "id":
                current_event["id"] = value


async def parse_chunked_stream(
    byte_stream: AsyncGenerator[bytes, None],
    delimiter: str = "\n"
) -> AsyncGenerator[str, None]:
    """
    Buffers and parses continuous byte stream deltas (e.g., Transfer-Encoding: chunked,
    NDJSON, or generic streaming buffers) into discreet delimited string records.
    """
    buffer = ""

    async for chunk in byte_stream:
        buffer += chunk.decode("utf-8", errors="replace")

        # Yield items as delimiters arrive
        while delimiter in buffer:
            item, buffer = buffer.split(delimiter, 1)
            item = item.strip()
            if item:
                yield item

    # Yield any remaining content left in buffer upon stream close
    remaining = buffer.strip()
    if remaining:
        yield remaining


# ============================================================================
# Preflight Ping Tool Using the Parsers
# ============================================================================

@mcp.tool()
async def preflight_ping(
    host: str,
    endpoint: str,
    method: str = "GET",
    token_env_var: str = "LOCUST_API_TOKEN",
    payload: Optional[Dict[str, Any]] = None,
    max_stream_chunks: int = 10,
    ctx: Context = None,
) -> Dict[str, Any]:
    """
    Makes a single authenticated preflight request to verify target reachability.
    Inspects response headers, routes through the SSE or Chunked parser,
    and emits MCP notifications/progress events per item received.
    """
    token = os.getenv(token_env_var)
    headers = {"Accept": "*/*"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = f"{host.rstrip('/')}/{endpoint.lstrip('/')}"
    start_time = time.perf_counter()

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            req = client.build_request(
                method=method.upper(),
                url=url,
                headers=headers,
                json=payload if payload else None,
            )
            response = await client.send(req, stream=True)
        except httpx.RequestError as exc:
            return {"status": "connection_error", "error": str(exc), "url": url}

        latency_to_first_byte = round((time.perf_counter() - start_time) * 1000, 2)
        content_type = response.headers.get("content-type", "").lower()
        transfer_encoding = response.headers.get("transfer-encoding", "").lower()

        is_sse = "text/event-stream" in content_type
        is_chunked = "chunked" in transfer_encoding or "application/x-ndjson" in content_type

        # ------------------------------------------------------------------
        # Branch 1: Parse Server-Sent Events (SSE)
        # ------------------------------------------------------------------
        if is_sse:
            captured_events: List[Dict[str, Any]] = []

            if ctx:
                await ctx.info(f"Connected to SSE stream (HTTP {response.status_code}). Reading events...")

            async for event in parse_sse_stream(response.aiter_bytes()):
                captured_events.append(event)
                idx = len(captured_events)

                if ctx:
                    await ctx.report_progress(progress=idx, total=max_stream_chunks)
                    data_preview = event["data"][:80].replace("\n", " ")
                    await ctx.info(f"SSE [event={event['event']}]: {data_preview}")

                if idx >= max_stream_chunks:
                    break

            await response.aclose()
            return {
                "status": "success" if response.is_success else "http_error",
                "http_status": response.status_code,
                "latency_to_first_byte_ms": latency_to_first_byte,
                "stream_type": "sse",
                "events_received": len(captured_events),
                "events": captured_events,
            }

        # ------------------------------------------------------------------
        # Branch 2: Parse Generic Chunked / NDJSON Streams
        # ------------------------------------------------------------------
        if is_chunked:
            captured_chunks: List[str] = []

            if ctx:
                await ctx.info(f"Connected to chunked stream (HTTP {response.status_code}). Reading chunks...")

            async for chunk in parse_chunked_stream(response.aiter_bytes()):
                captured_chunks.append(chunk)
                idx = len(captured_chunks)

                if ctx:
                    await ctx.report_progress(progress=idx, total=max_stream_chunks)
                    await ctx.info(f"Chunk #{idx}: {chunk[:80]}")

                if idx >= max_stream_chunks:
                    break

            await response.aclose()
            return {
                "status": "success" if response.is_success else "http_error",
                "http_status": response.status_code,
                "latency_to_first_byte_ms": latency_to_first_byte,
                "stream_type": "chunked",
                "chunks_received": len(captured_chunks),
                "chunks": captured_chunks,
            }

        # ------------------------------------------------------------------
        # Branch 3: Standard Non-Streaming Response
        # ------------------------------------------------------------------
        body_bytes = await response.aread()
        await response.aclose()

        return {
            "status": "success" if response.is_success else "http_error",
            "http_status": response.status_code,
            "latency_ms": latency_to_first_byte,
            "stream_type": "none",
            "response_snippet": body_bytes.decode("utf-8", errors="replace")[:500],
        }

@mcp.tool()
async def start_load_test(
    locustfile: str,
    host: str,
    users: int = 50,
    spawn_rate: int = 5,
    duration: str = "5m",
    token_env_var: str = "LOCUST_API_TOKEN"
) -> str:
    """Non-blocking start: launches Locust and returns run_id immediately."""
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    output_prefix = f"/tmp/locust_{run_id}"

    # Inherit environment and inject active token
    env = os.environ.copy()
    token = os.getenv(token_env_var)
    if token:
        env["LOCUST_API_TOKEN"] = token

    cmd = [
        "locust",
        "-f", locustfile,
        "--headless",
        "-H", host,
        "-u", str(users),
        "-r", str(spawn_rate),
        "-t", duration,
        f"--csv={output_prefix}",
        "--only-summary",
        "--stop-timeout", "10"
    ]

    # Non-blocking async process creation
    # CRITICAL for stdio: redirect stdout/stderr so raw text doesn't corrupt MCP pipe
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )

    ACTIVE_RUNS[run_id] = proc

    return (
        f"Started load test successfully.\n"
        f"Run ID: {run_id}\n"
        f"Target: {host} with {users} users for {duration}.\n"
        f"Use get_test_status(run_id='{run_id}') to inspect live progress."
    )


@mcp.tool()
async def get_test_status(run_id: str) -> str:
    """Checks if the run is still running or finished, reading live stats."""
    proc = ACTIVE_RUNS.get(run_id)
    if not proc:
        return f"Error: No active or recent process found for {run_id}."

    # returncode is None while process is still active
    is_running = (proc.returncode is None)

    stats_file = f"/tmp/locust_{run_id}_stats.csv"
    stats_preview = "No stats written to disk yet."
    if os.path.exists(stats_file):
        with open(stats_file, "r") as f:
            lines = f.readlines()
            # Return last line / summary row
            stats_preview = lines[-1].strip() if lines else stats_preview

    if is_running:
        return f"Status: RUNNING (PID {proc.pid})\nLatest Stats: {stats_preview}"
    else:
        ACTIVE_RUNS.pop(run_id, None)
        return f"Status: COMPLETED (Exit Code {proc.returncode})\nFinal Summary: {stats_preview}"


@mcp.tool()
async def stop_load_test(run_id: str) -> str:
    """Gracefully stops a test so Locust flushes final stats to CSV."""
    proc = ACTIVE_RUNS.get(run_id)
    if not proc or proc.returncode is not None:
        return f"Run {run_id} is not currently running."

    # Send SIGTERM so Locust gracefully shuts down and flushes final CSV reports
    proc.send_signal(signal.SIGTERM)
    await proc.wait()
    ACTIVE_RUNS.pop(run_id, None)
    return f"Stopped test {run_id}. Final stats are saved at /tmp/locust_{run_id}_stats.csv."

def get_locust_results(prefix: str) -> dict:
    """Reads Locust CSV outputs into raw dictionaries the agent can inspect."""
    results = {}
    for suffix in ("stats", "failures", "exceptions"):
        filepath = f"{prefix}_{suffix}.csv"
        if os.path.exists(filepath):
            with open(filepath, mode="r", encoding="utf-8") as f:
                results[suffix] = list(csv.DictReader(f))
    return results


if __name__ == "__main__":
    mcp.run(transport="stdio")