# ==============================================================================
# RECON.PY // CHUYÊN DÒ TÌM ENDPOINT (KHÔNG SPAM)
# ==============================================================================
import asyncio
import sys
from urllib.parse import urlparse
try:
    import httpx
except ImportError:
    print("[-] Cần cài: pip install httpx[http2]")
    sys.exit(1)

# Danh sách các đuôi phổ biến
ENDPOINTS = [
    "/search?q=",
    "/?s=",
    "/tim-kiem?keyword=",
    "/tim-kiem?q=",
    "/api/search?q=",
    "/product/search?q=",
    "/index.php?search=",
    "/catalogsearch/result/?q=", # Magento
    "/search/node?keys=", # Drupal
    "/search?keyword=",
]

async def recon(base_url: str):
    print(f"==================================================")
    print(f"[*] BẮT ĐẦU DÒ TÌM ENDPOINT: {base_url}")
    print(f"==================================================")
    
    # Giả lập User-Agent Chrome để không bị WAF chặn
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    }

    # Dùng HTTP/2, giới hạn kết nối thấp (chỉ dò thôi)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    
    found = []
    
    async with httpx.AsyncClient(http2=True, limits=limits, headers=headers, timeout=10.0, verify=False, follow_redirects=True) as client:
        for ep in ENDPOINTS:
            test_url = base_url + ep + "test123"
            try:
                resp = await client.get(test_url)
                # Nếu status 200 và độ dài trang lớn (có nội dung, không phải 404)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    print(f"[+] TÌM THẤY: {ep}")
                    print(f"    -> URL Đích: {base_url}{ep}")
                    print(f"    -> Độ dài HTML: {len(resp.text)} bytes")
                    found.append(ep)
                elif resp.status_code in (403, 429):
                    print(f"[!] BỊ CHẶN (WAF): {ep} (Status: {resp.status_code})")
            except Exception as e:
                pass # Lỗi thì bỏ qua, thử cái khác
    
    print(f"==================================================")
    if found:
        print(f"[*] HOÀN TẤT. Tìm thấy {len(found)} endpoint. Copy URL bên dưới để chém:")
        for ep in found:
            print(f"    -> {base_url}{ep}")
    else:
        print("[-] Đéo tìm thấy cái nào. Web này có thể xài React/Vue (API ngầm) hoặc WAF quá gắt.")
    print(f"==================================================")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 recon.py <url>")
        print("Ví dụ: python3 recon.py https://webmoi.com")
        sys.exit(1)
    u = urlparse(sys.argv[1])
    base = f"{u.scheme}://{u.hostname}"
    try:
        asyncio.run(recon(base))
    except KeyboardInterrupt:
        sys.exit(130)
