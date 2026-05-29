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

| Jedra (p) | Zagon 1 [s] | Zagon 2 [s] | Zagon 3 [s] | Povprečje T(p) [s] | Std [s] | Pospešek S(p) | Idealni S(p) | Karp-Flatt e | Strani | Zadetki |
|-----------|-------------|-------------|-------------|---------------------|---------|---------------|-------------|-------------|--------|---------|
| 1         | 33.99       | 10.52       | 11.48       | 18.66               | 13.28   | 1.0000        | 1.0         | 0.000000    | 51     | 25      |
| 2         | 14.60       | 6.49        | 6.35        | 9.15                | 4.73    | 2.0406        | 2.0         | -0.019875   | 51     | 33      |
| 4         | 8.96        | 4.78        | 4.63        | 6.12                | 2.45    | 3.0479        | 4.0         | 0.104125    | 51     | 29      |
| 8         | 8.38        | 3.04        | 3.06        | 4.83                | 3.08    | 3.8667        | 8.0         | 0.152711    | 51     | 35      |

### Pospešek S(p)

#### Primerjava dejanskega in idealnega pospeška

| Niti (p) | Dejanski pospešek S(p) | Idealni pospešek | Odmik |
|----------|------------------------|-------------------|-------|
| 1        | 1.00                   | 1                 | —     |
| 2        | 2.04                   | 2                 | +0.04 (super-linearen) |
| 4        | 3.05                   | 4                 | -0.95 |
| 8        | 3.87                   | 8                 | -4.13 |

Pospešek S(p) pove, kolikokrat hitreje teče program z več nitmi: `S(p) = T(1) / T(p)`. Idealno bi 2 niti pomenili 2x hitrejše, 4 niti 4x hitrejše itd. V praksi se to nikoli ne doseže popolnoma, ker del programa vedno teče zaporedno in ga ni mogoče paralelizirati (Amdahlov zakon).

Rezultati kažejo, da pospešek sledi idealnemu trendu pri manjšem številu niti, nato pa se odmik povečuje. Pri 2 nitih dosežemo pospešek 2.04x (celo rahlo super-linearen), pri 4 nitih 3.05x (namesto 4x), pri 8 nitih pa 3.87x (namesto 8x). Razlogi za odmik:

**1. I/O-bound narava naloge.** Scraping spletnih strani je pretežno omejen z mrežno latenco, ne s procesorsko močjo. Vsaka HTTP zahteva traja 100–500 ms (ali več), kar je neodvisno od števila CPU jeder. Tudi z več executorji čakamo na isti omrežni vmesnik in iste oddaljene strežnike, ki lahko omejujejo število sočasnih povezav z istega IP naslova (rate limiting).

**2. Spark overhead.** Za vsako iteracijo Spark ustvari RDD, serializira naloge, jih razpošlje executorjem in zbere rezultate. Ta režijski strošek je pri majhnem številu nalog (50 URL-jev) relativno velik glede na koristno delo.

**3. Neenakomerna porazdelitev dela (load imbalance).** Spletne strani so zelo različnih velikosti — nekatere vsebujejo na tisoče povezav in veliko besedila, druge so majhne ali nedosegljive. To pomeni, da nekateri executorji končajo delo mnogo prej kot drugi in čakajo.

![Pospešek](results/speedup.png)

### Karp-Flattova metrika e

#### Karp-Flatt metrika po številu niti

| Niti (p) | Karp-Flatt e | Pomen |
|----------|-------------|-------|
| 2        | -0.02       | Super-linearen pospešek (verjetno varianca meritev ali predpomnilnik) |
| 4        | 0.10        | ~10% programa je efektivno sekvenčnega |
| 8        | 0.15        | ~15% programa je efektivno sekvenčnega |

Karp-Flattova metrika e iz meritev izračuna dejanski delež programa, ki se obnaša sekvenčno: `e = (1/S(p) - 1/p) / (1 - 1/p)`. Za razliko od Amdahlovega zakona (ki predpostavlja fiksen sekvenčni delež) Karp-Flatt zajame tudi overhead, ki **narašča** z dodajanjem niti.

Če bi bil e konstanten, bi imeli fiksno ozko grlo (klasičen Amdahlov zakon). Naraščanje iz 0.10 na 0.15 pa kaže, da se overhead paralelizacije povečuje z dodajanjem niti — posledica Spark overheada za razporejanje nalog, mrežne konkurence in sinhronizacije med iteracijami. To ni fiksno sekvenčno ozko grlo, temveč rastoč strošek paralelizacije same.

Negativna vrednost pri 2 nitih (-0.02) pomeni, da je bil pospešek rahlo super-linearen (2.04x z 2 nitma), kar se lahko zgodi zaradi učinkov predpomnilnika operacijskega sistema ali naravne variance pri meritvah.

![Karp-Flatt](results/karp_flatt.png)

### Čas izvajanja

![Čas izvajanja](results/execution_time.png)

---

## Identifikacija ozkih grl

1. **Mrežna latenca:** Glavno ozko grlo. HTTP zahteve trajajo 10–100x dlje kot obdelava HTML-ja.
2. **Rate limiting:** Strežniki (npr. GitHub) omejujejo število zahtev na časovno enoto, kar umetno upočasni vzporedno izvajanje.
3. **Spark serializacija:** Overhead za razporejanje in zbiranje nalog pri majhnem obsegu dela.
4. **Deduplikacija med iteracijami:** Operaciji `distinct()` in `subtract()` zahtevata dodatno komunikacijo med executorji.
5. **Sinhronizacija med globinami:** BFS pristop zahteva, da se vse naloge na globini N zaključijo, preden se začne globina N+1. Hitrejši executorji čakajo na počasnejše.

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
