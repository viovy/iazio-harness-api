#!/usr/bin/env python3
"""Render one Gemini share page and print the visible transcript.

One process is started at a time by the API. The deadline is 45 seconds.
Chrome flags stay --no-sandbox and --disable-dev-shm-usage. driver.quit()
runs in a finally block.
"""

import os
import sys

DEADLINE_S = 45


def main() -> int:
    url = os.environ.get("SHARE_URL", "").strip()
    if not url.startswith("https://gemini.google.com/share/"):
        print("bad share url", file=sys.stderr)
        return 2
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        print("renderer unavailable", file=sys.stderr)
        return 3
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=opts)
    try:
        driver.set_page_load_timeout(DEADLINE_S)
        driver.get(url)
        text = driver.find_element("tag name", "body").text
        sys.stdout.write(text)
        return 0
    finally:
        driver.quit()


if __name__ == "__main__":
    raise SystemExit(main())
