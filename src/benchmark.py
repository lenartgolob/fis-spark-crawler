"""
Analiza zmogljivosti - Benchmarking
====================================
Izvede meritve časa za 1, 2, 4, 8 executorjev.
Za vsako konfiguracijo izvede 3 zaporedne zagone in izračuna povprečje.
Izračuna pospešek S(p) in Karp-Flattovo metriko e.
Rezultate shrani v CSV in generira grafe.
"""

import csv
import os
import sys
import json
import time
import statistics
from datetime import datetime

# Import crawlerja - dodamo pot do src mape
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from crawler import run_crawler


def compute_speedup(t1, tp):
    """Izračuna pospešek S(p) = T(1) / T(p)"""
    if tp == 0:
        return float("inf")
    return t1 / tp


def compute_karp_flatt(speedup, p):
    """
    Izračuna Karp-Flattovo metriko:
    e = (1/S(p) - 1/p) / (1 - 1/p)

    Manjša vrednost e pomeni boljšo paralelizacijo.
    """
    if p == 1:
        return 0.0
    numerator = (1.0 / speedup) - (1.0 / p)
    denominator = 1.0 - (1.0 / p)
    if denominator == 0:
        return float("inf")
    return numerator / denominator


def run_benchmark(seed_url, keyword, max_depth, core_configs, num_runs=3, max_urls=100):
    """
    Izvede celoten benchmark.

    Parametri:
        seed_url:       izhodiščni URL
        keyword:        ključna beseda
        max_depth:      globina crawla
        core_configs:   seznam konfiguracij jeder [1, 2, 4, 8]
        num_runs:       število ponovitev za vsako konfiguracijo
        max_urls:       maks. URL-jev na nivo globine

    Vrne:
        seznam dict-ov z rezultati
    """
    results = []
    crawl_logs = []

    for cores in core_configs:
        print(f"\n{'#'*60}")
        print(f"# Konfiguracija: {cores} jeder")
        print(f"{'#'*60}")

        run_times = []
        run_details = []

        for run_num in range(1, num_runs + 1):
            print(f"\n  Zagon {run_num}/{num_runs}...")
            result = run_crawler(seed_url, keyword, max_depth, cores, max_urls)
            elapsed = result["elapsed_time"]
            run_times.append(elapsed)
            run_details.append(result)
            crawl_logs.append({
                "cores": cores,
                "run": run_num,
                "total_runs": num_runs,
                "log_entries": result["log_entries"],
                "elapsed_time": result["elapsed_time"],
                "total_pages": result["total_pages"],
                "total_hits": result["total_hits"]
            })
            print(f"  -> Čas: {elapsed:.2f} s | Strani: {result['total_pages']} | Zadetki: {result['total_hits']}")

            # Kratka pavza med zagoni
            time.sleep(1)

        avg_time = statistics.mean(run_times)
        std_time = statistics.stdev(run_times) if len(run_times) > 1 else 0.0

        results.append({
            "cores": cores,
            "run_times": run_times,
            "avg_time": avg_time,
            "std_time": std_time,
            "total_pages": run_details[0]["total_pages"],
            "total_hits": run_details[0]["total_hits"]
        })

        print(f"\n  Povprečje: {avg_time:.2f} s (std: {std_time:.2f} s)")

    # Izračunaj pospešek in Karp-Flatt
    t1 = results[0]["avg_time"]  # Čas z 1 jedrom

    for r in results:
        r["speedup"] = compute_speedup(t1, r["avg_time"])
        r["karp_flatt"] = compute_karp_flatt(r["speedup"], r["cores"])
        r["ideal_speedup"] = float(r["cores"])

    return results, crawl_logs


def save_results_csv(results, filepath, num_runs=3):
    """Shrani rezultate v CSV datoteko."""
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["Jedra (p)"]
        for i in range(1, num_runs + 1):
            header.append(f"Zagon {i} [s]")
        header.extend([
            "Povprečje T(p) [s]", "Std [s]",
            "Pospešek S(p)", "Idealni pospešek",
            "Karp-Flatt e", "Strani", "Zadetki"
        ])
        writer.writerow(header)
        for r in results:
            row = [r["cores"]]
            for t in r["run_times"]:
                row.append(f"{t:.2f}")
            row.extend([
                f"{r['avg_time']:.2f}",
                f"{r['std_time']:.2f}",
                f"{r['speedup']:.4f}",
                f"{r['ideal_speedup']:.1f}",
                f"{r['karp_flatt']:.6f}",
                r["total_pages"],
                r["total_hits"]
            ])
            writer.writerow(row)
    print(f"\nRezultati shranjeni v: {filepath}")


def save_results_json(results, filepath):
    """Shrani rezultate v JSON za nadaljnjo obdelavo."""
    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)
    print(f"JSON rezultati shranjeni v: {filepath}")


def generate_plots(results, output_dir):
    """Generira grafe pospeška in Karp-Flatt metrike."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("OPOZORILO: matplotlib ni nameščen, grafi ne bodo generirani.")
        return

    cores = [r["cores"] for r in results]
    speedups = [r["speedup"] for r in results]
    ideal = [r["ideal_speedup"] for r in results]
    karp_flatt = [r["karp_flatt"] for r in results]
    avg_times = [r["avg_time"] for r in results]

    # ─── Graf 1: Pospešek ───
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(cores, speedups, "bo-", linewidth=2, markersize=8, label="Dejanski pospešek S(p)")
    ax.plot(cores, ideal, "r--", linewidth=1.5, label="Idealni pospešek (linearen)")
    ax.set_xlabel("Število jeder (p)", fontsize=12)
    ax.set_ylabel("Pospešek S(p)", fontsize=12)
    ax.set_title("Pospešek vzporednega crawlerja", fontsize=14)
    ax.legend(fontsize=11)
    ax.set_xticks(cores)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path1 = os.path.join(output_dir, "speedup.png")
    fig.savefig(path1, dpi=150)
    plt.close(fig)
    print(f"Graf pospeška shranjen: {path1}")

    # ─── Graf 2: Karp-Flatt metrika ───
    fig, ax = plt.subplots(figsize=(8, 5))
    kf_plot = [r["karp_flatt"] for r in results if r["cores"] > 1]
    cores_plot = [r["cores"] for r in results if r["cores"] > 1]
    ax.plot(cores_plot, kf_plot, "gs-", linewidth=2, markersize=8, label="Karp-Flatt e")
    ax.set_xlabel("Število jeder (p)", fontsize=12)
    ax.set_ylabel("Karp-Flatt metrika (e)", fontsize=12)
    ax.set_title("Karp-Flattova metrika", fontsize=14)
    ax.legend(fontsize=11)
    ax.set_xticks(cores_plot)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path2 = os.path.join(output_dir, "karp_flatt.png")
    fig.savefig(path2, dpi=150)
    plt.close(fig)
    print(f"Graf Karp-Flatt shranjen: {path2}")

    # ─── Graf 3: Čas izvajanja ───
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(range(len(cores)), avg_times, tick_label=[str(c) for c in cores],
           color="steelblue", edgecolor="black")
    ax.set_xlabel("Število jeder (p)", fontsize=12)
    ax.set_ylabel("Povprečni čas T(p) [s]", fontsize=12)
    ax.set_title("Čas izvajanja glede na število jeder", fontsize=14)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    path3 = os.path.join(output_dir, "execution_time.png")
    fig.savefig(path3, dpi=150)
    plt.close(fig)
    print(f"Graf časa izvajanja shranjen: {path3}")


def print_summary(results):
    """Izpiše povzetek rezultatov v terminalu."""
    print(f"\n{'='*70}")
    print(f"  POVZETEK ANALIZE ZMOGLJIVOSTI")
    print(f"{'='*70}")
    print(f"  {'Jedra':>6} | {'T(p) [s]':>10} | {'S(p)':>8} | {'Idealni':>8} | {'Karp-Flatt e':>13}")
    print(f"  {'-'*6}-+-{'-'*10}-+-{'-'*8}-+-{'-'*8}-+-{'-'*13}")
    for r in results:
        print(f"  {r['cores']:>6} | {r['avg_time']:>10.2f} | {r['speedup']:>8.4f} | {r['ideal_speedup']:>8.1f} | {r['karp_flatt']:>13.6f}")
    print(f"{'='*70}\n")


# ──────────────────────────────────────────────
# Vstopna točka
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Benchmark vzporednega crawlerja"
    )
    parser.add_argument("--seed-url", type=str, required=True,
                        help="Izhodiščni URL naslov")
    parser.add_argument("--keyword", type=str, required=True,
                        help="Ključna beseda za iskanje")
    parser.add_argument("--max-depth", type=int, default=2,
                        help="Maksimalna globina (privzeto: 2)")
    parser.add_argument("--cores", type=str, default="1,2,4,8",
                        help="Konfiguracije jeder, ločene z vejico (privzeto: 1,2,4,8)")
    parser.add_argument("--runs", type=int, default=3,
                        help="Število ponovitev (privzeto: 3)")
    parser.add_argument("--output-dir", type=str, default="results",
                        help="Mapa za rezultate (privzeto: results)")
    parser.add_argument("--max-urls", type=int, default=100,
                        help="Maks. URL-jev na nivo globine (privzeto: 100)")

    args = parser.parse_args()
    core_configs = [int(c) for c in args.cores.split(",")]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join(args.output_dir, timestamp)
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  BENCHMARK: Vzporedni Link Crawler")
    print(f"  Seed URL:       {args.seed_url}")
    print(f"  Keyword:        {args.keyword}")
    print(f"  Max depth:      {args.max_depth}")
    print(f"  Max URLs/nivo:  {args.max_urls}")
    print(f"  Konfiguracije:  {core_configs}")
    print(f"  Ponovitev:      {args.runs}")
    print(f"  Output:         {output_dir}")
    print(f"{'='*60}")

    results, crawl_logs = run_benchmark(
        args.seed_url, args.keyword, args.max_depth,
        core_configs, args.runs, args.max_urls
    )

    print_summary(results)
    save_results_csv(results, os.path.join(output_dir, "benchmark_results.csv"), args.runs)
    save_results_json(results, os.path.join(output_dir, "benchmark_results.json"))
    generate_plots(results, output_dir)

    log_path = os.path.join(output_dir, "crawl.log")
    with open(log_path, "w") as f:
        for log in crawl_logs:
            f.write(f"\n{'='*60}\n")
            f.write(f"  CORES: {log['cores']} | RUN: {log['run']}/{log['total_runs']}\n")
            f.write(f"{'='*60}\n")
            current_depth = None
            for entry in log["log_entries"]:
                if entry["depth"] != current_depth:
                    current_depth = entry["depth"]
                    count = sum(1 for e in log["log_entries"] if e["depth"] == current_depth)
                    f.write(f"[DEPTH {current_depth}] Processing {count} URLs\n")
                status = entry["status"]
                kw = "YES" if entry["keyword_found"] else "NO"
                if isinstance(status, int):
                    label = "OK" if status < 400 else "FAIL"
                    f.write(f"  {label:4s} {status}  {entry['url']}  keyword={kw}  links={entry['links_found']}\n")
                else:
                    f.write(f"  ERR  ---  {entry['url']}  {status}\n")
            f.write(f"Finished: {log['elapsed_time']:.2f}s | Pages: {log['total_pages']} | Hits: {log['total_hits']}\n")
    print(f"\nCrawl log shranjen: {log_path}")
