# Vzporedni spletni pajek s sledenjem povezav (Link Crawler)

**Predmet:** Visoko-zmogljivo računalništvo  
**Tehnologija:** Apache Spark (PySpark)

---

## Opis problema

Program implementira vzporedni spletni pajek (web crawler), ki začne z enim izhodiščnim URL naslovom in iterativno sledi hiperpovezavam do vnaprej določene globine. Na vsaki obiskani strani preveri prisotnost izbrane ključne besede. Cilj je demonstrirati uporabo Apache Spark za dinamično upravljanje nalog in porazdeljeno obdelavo nestrukturiranih podatkov z interneta.

### Spark naloga

V vsaki iteraciji (nivo globine) se ustvari RDD iz trenutnega seznama neobiskanih URL-jev. Z uporabo `map` vsak executor vzporedno obišče svojo stran, izvleče hiperpovezave in preveri prisotnost ključne besede. Novo najdene URL-je program zbere, odstrani duplikate z `distinct()` in jih primerja z množico že obiskanih naslovov (`subtract()`), da dobi seznam za naslednjo iteracijo.

### Ključni koncepti
- **Iterativna BFS strategija:** vsak nivo globine = en Spark cikel
- **Deduplikacija:** `distinct()` + `subtract()` za preprečevanje večkratnega obiska
- **Broadcast spremenljivke:** ključna beseda se pošlje executorjem enkrat
- **Dinamično particioniranje:** `numSlices` se prilagodi številu jeder

---

## Struktura projekta

```
spark-crawler/
├── src/
│   ├── crawler.py        # Glavni PySpark crawler
│   └── benchmark.py      # Analiza zmogljivosti
├── results/
│   ├── benchmark_results.csv   # Tabela meritev
│   ├── benchmark_results.json  # Rezultati v JSON
│   ├── speedup.png             # Graf pospeška
│   ├── karp_flatt.png          # Graf Karp-Flatt metrike
│   └── execution_time.png      # Graf časa izvajanja
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Namestitev in zagon

### Zagon z Dockerjem (priporočeno)

Predpogoj: Docker.

```bash
docker build -t spark-crawler .
docker run --rm -v "$(pwd)/results:/app/results" spark-crawler
```

Privzeti parametri so nastavljeni v `Dockerfile`. Za prilagoditev:
```bash
docker run --rm -v "$(pwd)/results:/app/results" spark-crawler \
  python src/benchmark.py \
  --seed-url "https://github.com/apache/spark" \
  --keyword "apache" \
  --max-depth 2 \
  --max-urls 50 \
  --cores "1,2,4,8" \
  --runs 3 \
  --output-dir results
```

### Zagon brez Dockerja

Predpogoji: Python 3.10+ in Java 11+.

```bash
pip install -r requirements.txt
python src/crawler.py \
  --seed-url "https://github.com/apache/spark" \
  --keyword "apache" \
  --max-depth 2 \
  --max-urls 50 \
  --cores 4
```

### Parametri
| Parameter      | Opis                                      | Privzeto |
|----------------|-------------------------------------------|----------|
| `--seed-url`   | Izhodiščni URL naslov                     | (obvezen)|
| `--keyword`    | Ključna beseda za iskanje                 | (obvezen)|
| `--max-depth`  | Maksimalna globina preiskovanja           | 2        |
| `--max-urls`   | Maks. število URL-jev na nivo globine     | 100      |
| `--cores`      | Število jeder (executorjev)               | 1        |

---

## Rezultati meritev

### Testna konfiguracija
- **Seed URL:** `https://github.com/apache/spark`
- **Ključna beseda:** `apache`
- **Globina:** 2 (seed + 1 nivo povezav)
- **Maks. URL-jev na nivo:** 50
- **Število zagonov:** 3 (za povprečje)
- **Okolje:** Docker (Python 3.11, OpenJDK 21)

### Tabela rezultatov

| Jedra (p) | Zagon 1 [s] | Zagon 2 [s] | Zagon 3 [s] | Povprečje T(p) [s] | Pospešek S(p) | Idealni S(p) | Karp-Flatt e |
|-----------|-------------|-------------|-------------|---------------------|---------------|-------------|-------------|
| 1         | 29.52       | 11.30       | 11.19       | 17.34               | 1.0000        | 1.0         | 0.000000    |
| 2         | 13.42       | 6.48        | 6.69        | 8.86                | 1.9561        | 2.0         | 0.022428    |
| 4         | 8.79        | 4.62        | 4.49        | 5.97                | 2.9054        | 4.0         | 0.125582    |
| 8         | 5.49        | 3.53        | 3.52        | 4.18                | 4.1477        | 8.0         | 0.132681    |

### Grafi

#### Pospešek S(p)
![Pospešek](results/speedup.png)

#### Karp-Flattova metrika e
![Karp-Flatt](results/karp_flatt.png)

#### Čas izvajanja
![Čas izvajanja](results/execution_time.png)

---

## Interpretacija rezultatov

### Odmik od idealnega pospeška

Rezultati kažejo, da pospešek sledi idealnemu trendu pri manjšem številu jeder, nato pa se odmik povečuje. Pri 2 jedrih dosežemo pospešek 1.96x (skoraj idealna 2x), pri 4 jedrih 2.91x (namesto 4x), pri 8 jedrih pa 4.15x (namesto 8x). Razlogi za odmik:

**1. I/O-bound narava naloge.** Scraping spletnih strani je pretežno omejen z mrežno latenco, ne s procesorsko močjo. Vsaka HTTP zahteva traja 100–500 ms (ali več), kar je neodvisno od števila CPU jeder. Tudi z več executorji čakamo na isti omrežni vmesnik in iste oddaljene strežnike, ki lahko omejujejo število sočasnih povezav z istega IP naslova (rate limiting).

**2. Spark overhead.** Za vsako iteracijo Spark ustvari RDD, serializira naloge, jih razpošlje executorjem in zbere rezultate. Ta režijski strošek je pri majhnem številu nalog (50 URL-jev) relativno velik glede na koristno delo.

**3. Neenakomerna porazdelitev dela (load imbalance).** Spletne strani so zelo različnih velikosti — nekatere vsebujejo na tisoče povezav in veliko besedila, druge so majhne ali nedosegljive. To pomeni, da nekateri executorji končajo delo mnogo prej kot drugi in čakajo.

### Trend Karp-Flattove metrike

Karp-Flattova metrika e narašča od 0.02 pri 2 jedrih do 0.13 pri 4 jedrih, nato pa se ustali pri 0.13 tudi pri 8 jedrih. Začetna rast pomeni, da se efektivni delež sekvenčnega dela povečuje z dodajanjem jeder — posledica Spark overheada in mrežnih omejitev. Plateau med 4 in 8 jedri kaže, da overhead ne narašča več, ampak obstaja trda zgornja meja paralelizacije zaradi I/O ozkega grla.

### Identifikacija ozkih grl

1. **Mrežna latenca:** Glavno ozko grlo. HTTP zahteve trajajo 10–100x dlje kot obdelava HTML-ja.
2. **Rate limiting:** Strežniki (npr. GitHub) omejujejo število zahtev na časovno enoto, kar umetno upočasni vzporedno izvajanje.
3. **Spark serializacija:** Overhead za razporejanje in zbiranje nalog pri majhnem obsegu dela.
4. **Deduplikacija med iteracijami:** Operaciji `distinct()` in `subtract()` zahtevata dodatno komunikacijo med executorji.

### Možne izboljšave

- Uporaba **asinhronih HTTP zahtev** (aiohttp) namesto sinhronih, kar bi zmanjšalo vpliv mrežne latence
- Povečanje obsega naloge (več URL-jev), kar bi izboljšalo razmerje med koristnim delom in overheadom
- Uporaba **persistentne množice obiskanih URL-jev** z broadcast spremenljivkami namesto collect/parallelize cikla
- Predpomnjenje DNS poizvedb za zmanjšanje latence

---

## Formule

**Pospešek:**
```
S(p) = T(1) / T(p)
```

**Karp-Flattova metrika:**
```
e = (1/S(p) - 1/p) / (1 - 1/p)
```

Kjer je `p` število jeder, `T(1)` čas z enim jedrom in `T(p)` čas s `p` jedri.
