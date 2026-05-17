"""
Vzporedni spletni pajek s sledenjem povezav (Link Crawler)
============================================================
Visoko-zmogljivo računalništvo - Seminarska naloga

Uporaba Apache Spark (PySpark) za vzporedno preiskovanje spletnih strani.
Program začne z enim izhodiščnim URL-jem, iterativno sledi povezavam do
določene globine in išče prisotnost ključne besede na vsaki strani.
"""

import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pyspark.sql import SparkSession


# ──────────────────────────────────────────────
# Pomožne funkcije (izvajajo se na executorjih)
# ──────────────────────────────────────────────

def fetch_and_parse(url, keyword):
    """
    Obišče eno spletno stran:
      - Prenese HTML vsebino
      - Preveri prisotnost ključne besede
      - Izvleče vse hiperpovezave (href)

    Vrne tuple: (url, keyword_found, seznam_novih_povezav)
    """
    try:
        headers = {"User-Agent": "SparkCrawler/1.0 (university project)"}
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()

        # Preverimo samo HTML vsebino
        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            return (url, False, [])

        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        # Preveri ključno besedo (case-insensitive)
        text_content = soup.get_text().lower()
        keyword_found = keyword.lower() in text_content

        # Izvleči vse povezave
        links = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            full_url = urljoin(url, href)
            parsed = urlparse(full_url)
            # Obdrži samo http/https povezave
            if parsed.scheme in ("http", "https"):
                # Odstrani fragment (#)
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if parsed.query:
                    clean_url += f"?{parsed.query}"
                links.append(clean_url)

        return (url, keyword_found, links)

    except Exception as e:
        # Stran ni dosegljiva ali napaka pri parsanju
        return (url, False, [])


# ──────────────────────────────────────────────
# Glavna Spark logika crawlerja
# ──────────────────────────────────────────────

def run_crawler(seed_url, keyword, max_depth, num_cores, max_urls_per_depth=100):
    """
    Izvede vzporedni crawl z Apache Spark.

    Parametri:
        seed_url:           izhodiščni URL naslov
        keyword:            ključna beseda za iskanje
        max_depth:          maksimalna globina preiskovanja
        num_cores:          število executorjev (local[N])
        max_urls_per_depth: maks. število URL-jev na nivo (za nadzor obsega)

    Vrne:
        dict s statistiko in časom izvajanja
    """
    spark = (
        SparkSession.builder
        .master(f"local[{num_cores}]")
        .appName(f"LinkCrawler-{num_cores}cores")
        .config("spark.ui.enabled", "false")       # Onemogočimo UI za manj overheada
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Dodaj to datoteko kot py file, da jo workerji lahko najdejo
    import os as _os
    _this_file = _os.path.abspath(__file__)
    sc.addPyFile(_this_file)

    # Broadcast ključne besede (da se ne pošilja z vsako nalogo)
    kw_broadcast = sc.broadcast(keyword)

    visited = set()         # Množica že obiskanih URL-jev
    to_visit = [seed_url]   # Vrsta URL-jev za obdelavo
    all_results = []        # Vsi rezultati po nivojih
    stats_per_depth = []    # Statistika po globini

    start_time = time.time()

    for depth in range(max_depth):
        # Filtriraj URL-je, ki so že obiskani
        new_urls = [u for u in to_visit if u not in visited]
        if not new_urls:
            print(f"  Globina {depth}: ni novih URL-jev, zaključujem.")
            break

        # Omeji število URL-jev na nivo
        if max_urls_per_depth and len(new_urls) > max_urls_per_depth:
            new_urls = new_urls[:max_urls_per_depth]

        print(f"  Globina {depth}: obdelujem {len(new_urls)} URL-jev z {num_cores} jedri...")

        # Ustvari RDD iz seznama URL-jev in vzporedno obdelaj
        urls_rdd = sc.parallelize(new_urls, numSlices=min(len(new_urls), num_cores * 2))

        kw = kw_broadcast.value
        results_rdd = urls_rdd.map(lambda url: fetch_and_parse(url, kw))

        # Zberi rezultate
        results = results_rdd.collect()

        # Posodobi statistiko
        depth_hits = 0
        next_level_urls = []
        for url, found, links in results:
            visited.add(url)
            all_results.append({"url": url, "keyword_found": found, "depth": depth})
            if found:
                depth_hits += 1
            next_level_urls.extend(links)

        stats_per_depth.append({
            "depth": depth,
            "pages_crawled": len(new_urls),
            "keyword_hits": depth_hits
        })

        # Deduplikacija z uporabo Spark RDD operacij
        # Uporabimo distinct() in subtract() za učinkovito odstranitev duplikatov
        if next_level_urls:
            next_rdd = sc.parallelize(next_level_urls).distinct()
            visited_rdd = sc.parallelize(list(visited))
            to_visit = next_rdd.subtract(visited_rdd).collect()
        else:
            to_visit = []

    elapsed = time.time() - start_time

    # Končna statistika
    total_pages = len(all_results)
    total_hits = sum(1 for r in all_results if r["keyword_found"])

    spark.stop()

    return {
        "num_cores": num_cores,
        "elapsed_time": elapsed,
        "total_pages": total_pages,
        "total_hits": total_hits,
        "stats_per_depth": stats_per_depth,
        "results": all_results
    }


# ──────────────────────────────────────────────
# Vstopna točka
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Vzporedni spletni pajek s sledenjem povezav (PySpark)"
    )
    parser.add_argument("--seed-url", type=str, required=True,
                        help="Izhodiščni URL naslov")
    parser.add_argument("--keyword", type=str, required=True,
                        help="Ključna beseda za iskanje")
    parser.add_argument("--max-depth", type=int, default=2,
                        help="Maksimalna globina preiskovanja (privzeto: 2)")
    parser.add_argument("--cores", type=int, default=1,
                        help="Število jeder (privzeto: 1)")
    parser.add_argument("--max-urls", type=int, default=100,
                        help="Maks. URL-jev na nivo globine (privzeto: 100)")

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Vzporedni Link Crawler (PySpark)")
    print(f"  Seed URL:   {args.seed_url}")
    print(f"  Keyword:    {args.keyword}")
    print(f"  Max depth:  {args.max_depth}")
    print(f"  Max URLs:   {args.max_urls}")
    print(f"  Cores:      {args.cores}")
    print(f"{'='*60}\n")

    result = run_crawler(args.seed_url, args.keyword, args.max_depth, args.cores, args.max_urls)

    print(f"\n{'='*60}")
    print(f"  REZULTATI")
    print(f"{'='*60}")
    print(f"  Obiskanih strani:  {result['total_pages']}")
    print(f"  Zadetkov besede:   {result['total_hits']}")
    print(f"  Čas izvajanja:     {result['elapsed_time']:.2f} s")
    print()
    for s in result["stats_per_depth"]:
        print(f"  Globina {s['depth']}: {s['pages_crawled']} strani, {s['keyword_hits']} zadetkov")
    print(f"{'='*60}\n")
