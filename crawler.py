#
# Dominik Mifkovič 2025
#
import os
import sys
import re
import time
import html
import random
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

visited_pages = set()
discovered_links = set()
robots_parsers = {}

BASE_URL = "https://whc.unesco.org/en/"

#vygenerovane cez https://useragents.io/random
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.71 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.71 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/129.0.2792.52"
]

#funkcia na vytvorenie headless chrome drivera
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    #neviem ci toto realne daco robi ked je driver headless ale pre istotu to necham :DD
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-software-rasterizer")
    #zabezpeci ze sa nebudu logovat zbytocne informacie
    options.add_argument("--log-level=3")
    options.add_argument("user-agent=" + random.choice(USER_AGENTS))

    #instaluje a spusti chromedriver
    service = Service(ChromeDriverManager().install(), service_args=['--silent'])

    #vytvori webdriver
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(20)
    return driver

#zmeni user-agent pocas behu
def rotate_user_agent(driver):
    user_agent = random.choice(USER_AGENTS)
    #pouzije chrome devtools prikaz na zmenu agenta lebo vytvaranie noveho drivera bolo prilis pomale
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agent})

#vrati True ak je dane URL dovolene podla robots.txt
def allowed_by_robots(url):
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base not in robots_parsers:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(urljoin(base, "/robots.txt"))
        try:
            rp.read()
        except Exception:
            #ak sa robots.txt neda nacitat, predpokladame ze mozeme
            robots_parsers[base] = None
            return True
        robots_parsers[base] = rp
    rp = robots_parsers[base]
    if rp is None:
        return True
    return rp.can_fetch("*", url)

#vrati True ak URL patri do rovnakej domeny ako BASE_URL
def in_same_domain(url):
    base = BASE_URL.rstrip('/')
    target = url.rstrip('/')
    return target.startswith(base)


#vycisti query parametre
def clean_url(url):
    url = html.unescape(url)
    parsed = urlparse(url)
    path = parsed.path
    #odstrani query parametre a fragmenty
    path = re.split(r'[?&;=%]', path)[0]
    normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
    if normalized.endswith('/') and normalized != BASE_URL.rstrip('/'):
        normalized = normalized[:-1]
    
    return normalized


def extract_links(driver, page_url):
    normalized = clean_url(page_url)
    try:
        driver.get(normalized)
        html_source = driver.page_source

        os.makedirs("pages", exist_ok=True)
        file_name = re.sub(r'[^A-Za-z0-9_\-\.]', '_', normalized.replace('https://', '').replace('http://', ''))
        file_path = os.path.join("pages", f"{file_name}.html")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_source)
            
        #najdeme vsetky odkazy na stranke
        hrefs = re.findall(r'href=["\'](.*?)["\']', html_source, re.IGNORECASE)
        new_links = []

        for href in hrefs:
            href = html.unescape(href)
            absolute = urljoin(normalized, href)
            normalized_link = clean_url(absolute)
            #ignorujeme odkazy mimo domeny
            if not in_same_domain(normalized_link):
                continue
            if normalized_link not in discovered_links:
                discovered_links.add(normalized_link)
                new_links.append(normalized_link)
                #zapise novy link do suboru hned po objaveni
                with open("links.txt", "a", encoding="utf-8") as lf:
                    lf.write(f"{html.escape(normalized_link)}\n")

        return new_links

    except Exception as e:
        return []


def crawl(start_url):
    driver = create_driver()
    try:
        if discovered_links:
            to_visit = [link for link in discovered_links if link not in visited_pages]
        else:
            to_visit = [start_url]

        #hlavny crawl cyklus
        while to_visit:
            current_url = to_visit.pop(0)
            normalized = clean_url(current_url)
            if normalized in visited_pages:
                continue

            #kontrola podla robots.txt
            if not allowed_by_robots(normalized):
                continue

            #oznacime ako navstivenu
            visited_pages.add(normalized)
            #ziskame nove linky zo stranky a pridame ich do fronty
            new_links = extract_links(driver, current_url)
            to_visit.extend(new_links)

            #ziskame aktualny user agent
            current_agent = driver.execute_script("return navigator.userAgent;")
            print(f"\r\033[KFound: {len(discovered_links)} | Visited: {len(visited_pages)} | Last: {normalized} | Agent: {current_agent}", end="", flush=True)

            time.sleep(random.uniform(1.0, 3.0))
            rotate_user_agent(driver)
    except Exception as e:
        #print(f"\n{e}")
        pass
    finally:
        driver.quit()

def main():
    #aby chyby nezahlcovali konzolu
    sys.stderr = open(os.devnull, 'w')
    
    #nacitanie uz znamych linkov z links.txt
    if os.path.exists("links.txt"):
        with open("links.txt", "r", encoding="utf-8") as f:
            for line in f:
                link = line.strip()
                if link:
                    discovered_links.add(link)

    #nacitanie uz navstivenych stranok z adresara pages/
    if os.path.exists("pages"):
        for filename in os.listdir("pages"):
            if filename.endswith(".html"):
                name = filename[:-5]  #odstrani .html
                #nahradi _ za / a prida prefix https://
                visited_url = "https://" + name.replace('_', '/')
                visited_pages.add(visited_url)

    start_time = time.time()
    print("Starting crawler...")
    crawl(BASE_URL)
    elapsed = time.time() - start_time

    print("\n\nCrawl complete.")
    print(f"Total unique links found: {len(discovered_links)}")
    print(f"Pages visited: {len(visited_pages)}")
    print("Links saved to links.txt")
    print(f"Total runtime: {elapsed:.2f} seconds")

main()
