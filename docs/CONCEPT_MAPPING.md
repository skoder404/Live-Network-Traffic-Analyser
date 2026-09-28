# LNTA Core Algorithmic Concepts & Architectural Mapping

**Date:** September 29, 2026  
**Author:** Yashwant Vadhan M (Integration & Advanced Analytics)  
**System:** Live Network Traffic Analyser (LNTA)  

---

## 1. Master Algorithmic & Architectural Concept Map

| Concept | Plain-English Meaning | Implementation Path | Dashboard Location | Exact vs Approximate Comparison | Visual Reference |
|---|---|---|---|---|---|
| **Stream Data Model** | Continuous, append-only, high-velocity tuple stream with event-time watermarking and micro-batch partitioning. | [`streaming/common/schema.py`](../streaming/common/schema.py), [`streaming/stream_app.py`](../streaming/stream_app.py) | **Live Overview** & **Pipeline Health** | Real-time structured streaming with 30s watermark vs static batch capture. | `docs/img/architecture.svg` |
| **Stream Filtering** | Evaluating boolean predicates and Bloom filter bit arrays on unbounded streams without buffering. | [`streaming/analytics/filters.py`](../streaming/analytics/filters.py), [`streaming/queries/filter_counts.py`](../streaming/queries/filter_counts.py) | **Stream Analytics (Group A)** → *Filtering Panel* | Deterministic SQL filters vs Probabilistic Bloom Filter ($m=10000, k=5$) with false-positive tracking ($< 1\%$). | `docs/img/dashboard_stream_analytics.png` |
| **Reservoir Sampling** | Maintaining a statistically unbiased fixed-capacity random sample over an infinite stream of unknown size. | [`streaming/analytics/sampling.py`](../streaming/analytics/sampling.py) | **Stream Analytics (Group A)** → *Sampling Panel* | Full-stream mean packet length vs Reservoir ($K=500$) sample mean fidelity ($< 3\%$ error). | `docs/img/dashboard_stream_analytics.png` |
| **Count Distinct (HLL & FM)** | Estimating the number of unique elements (cardinality) in sub-linear memory using hash bit patterns. | [`streaming/analytics/distinct.py`](../streaming/analytics/distinct.py), [`streaming/analytics/fm.py`](../streaming/analytics/fm.py) | **Stream Analytics (Group A)** → *Distinct IPs Panel* | Exact `COUNT(DISTINCT)` vs HyperLogLog ($b=10$) and Flajolet-Martin trailing-zero estimator ($\approx 5\%$ error). | `docs/img/dashboard_stream_analytics.png` |
| **Counting Ones (DGIM)** | Estimating the number of '1' bits in a sliding binary window using exponentially sized logarithmic bucket buckets. | [`streaming/analytics/counting_ones.py`](../streaming/analytics/counting_ones.py) | **Stream Analytics (Group A)** → *DGIM Counter* | Exact sliding window 1-bit count vs DGIM bucket power-of-2 representation ($< 15\%$ error bound). | `docs/img/dashboard_stream_analytics.png` |
| **Estimating Moments (AMS F2)** | Estimating the second frequency moment ($F_2 = \sum c_i^2$, surprise / energy of data stream) via randomized $\pm 1$ hashing. | [`streaming/analytics/moments.py`](../streaming/analytics/moments.py), [`streaming/queries/moments_window.py`](../streaming/queries/moments_window.py) | **Stream Analytics (Group B)** → *Moments Panel* | Exact packet length second moment vs Alon-Matias-Szegedy ($k=30$ hash seeds) $F_2$ estimator ($< 8\%$ error). | `docs/img/dashboard_stream_analytics.png` |
| **Exponentially Decaying Window** | Tracking recent traffic activity by weighting events smoothly with exponential decay factor $\lambda$, obsoleting hard cutoff boundaries. | [`streaming/analytics/decay.py`](../streaming/analytics/decay.py) | **Live Overview** & **Stream Analytics (Group B)** | Sliding 60s moving average sum vs Exponential decay score ($s_{t} = s_{t-1} e^{-\lambda \Delta t} + 1$). | `docs/img/dashboard_overview.png` |
| **Market-Basket Analysis (A-Priori)** | Discovering frequent protocol-port service co-occurrences using candidate generation and support pruning. | [`streaming/analytics/itemsets.py`](../streaming/analytics/itemsets.py) | **Stream Analytics (Group B)** → *Frequent Itemsets* | Full 2-pass frequent itemset candidate evaluation vs Exact frequency counts. | `docs/img/dashboard_stream_analytics.png` |
| **Limited-Pass Frequent Itemsets (PCY)** | Reducing candidate pair memory footprint during pass 1 using hash bucket bitmaps. | [`streaming/analytics/itemsets.py`](../streaming/analytics/itemsets.py) | **Stream Analytics (Group B)** → *Frequent Itemsets* | A-Priori candidate count vs PCY candidate pair count ($> 40\%$ candidate reduction). | `docs/img/dashboard_stream_analytics.png` |
| **IP Communication Graph & Centrality** | Modeling network hosts and directional communications as a weighted directed multigraph ($G=(V,E)$). | [`linkanalysis/graph.py`](../linkanalysis/graph.py), [`linkanalysis/centrality.py`](../linkanalysis/centrality.py) | **Link Analysis** → *Topology Graph & Centrality Table* | In-degree, Out-degree, Weighted Volume, and Betweenness Centrality ($N \le 300$). | `docs/img/dashboard_linkanalysis.png` |
| **PageRank (NetworkX & Power Iteration)** | Computing node structural authority in communication graphs via random walk transition convergence with teleportation. | [`linkanalysis/pagerank.py`](../linkanalysis/pagerank.py) | **Link Analysis** → *PageRank Ranking Table* | NetworkX `nx.pagerank` vs Custom power-iteration implementation ($\max(\Delta) < 10^{-4}$). | `docs/img/dashboard_linkanalysis.png` |
| **Markov Transition Model & Surprise** | Modeling 1-step packet transition probabilities between host IP states and detecting anomalous link hops. | [`linkanalysis/markov.py`](../linkanalysis/markov.py) | **Link Analysis** → *Markov Transitions Heatmap* | Empirical transition probability $P_{uv}$ vs Information surprise score in bits ($-\log_2 P_{uv}$). | `docs/img/dashboard_linkanalysis.png` |

---

## 2. In-Depth Algorithmic Implementations

### 2.1 Stream Data Model & Ingestion
- **Concept:** Continuous packet streaming with event-time timestamps, late arrival watermarking ($30\text{s}$), and dual-lane processing (Lane A: native SQL streaming; Lane B: micro-batch stateful algorithmic plugins).
- **File:** [`streaming/stream_app.py`](../streaming/stream_app.py), [`streaming/common/schema.py`](../streaming/common/schema.py)
- **Serving Table:** `window_metrics`, `pipeline_health`

### 2.2 Stream Filtering & Bloom Filters
- **Concept:** Membership verification on unbounded network streams. Evaluates boolean predicates and maintains a bit array with independent hash seeds.
- **File:** [`streaming/analytics/filters.py`](../streaming/analytics/filters.py)
- **Comparison:** Exact SQL filtering (`protocol = 'TCP'`) vs Probabilistic Bloom Filter false-positive rates ($FPR < 1\%$).

### 2.3 Reservoir Sampling (Alan G. Waterman / Vitter's Algorithm R)
- **Concept:** Selects a uniform random sample of size $K=500$ from an infinite stream where total count $N$ is unknown beforehand.
- **File:** [`streaming/analytics/sampling.py`](../streaming/analytics/sampling.py)
- **Comparison:** Sample mean packet length vs Stream ground truth mean length ($\text{Error} < 3\%$).

### 2.4 Count Distinct: HyperLogLog & Flajolet-Martin
- **Concept:** Cardinality estimation of distinct IP addresses using maximum trailing zeros ($\rho$) in hashed representations.
- **File:** [`streaming/analytics/distinct.py`](../streaming/analytics/distinct.py), [`streaming/analytics/fm.py`](../streaming/analytics/fm.py)
- **Comparison:** Exact `COUNT(DISTINCT dst_ip)` vs HLL ($b=10$, $m=1024$) and Flajolet-Martin ($2^{\text{median}(R)}$).

### 2.5 Counting Ones: DGIM Algorithm (Datar-Gionis-Indyk-Motwani)
- **Concept:** Logarithmic bucket allocation representing counts of $1$-bits within a sliding window of size $N=1000$. Guarantees relative error $\le 1/(2k)$.
- **File:** [`streaming/analytics/counting_ones.py`](../streaming/analytics/counting_ones.py)
- **Comparison:** Sliding window exact bit counter vs DGIM estimated sum.

### 2.6 Moment Estimation: Alon-Matias-Szegedy (AMS) $F_2$ Estimator
- **Concept:** Estimating second frequency moment $F_2 = \sum f_i^2$ (variance/energy) using 4-way independent hash functions mapping stream tokens to $\{-1, +1\}$.
- **File:** [`streaming/analytics/moments.py`](../streaming/analytics/moments.py)
- **Comparison:** Exact $F_2 = \sum \text{len}^2$ vs AMS estimator ($k=30$ random seeds).

### 2.7 Exponential Decaying Window
- **Concept:** Smooth time-decaying sum where older packets exponentially diminish in weight ($w_i = c^{- \lambda (t - t_i)}$).
- **File:** [`streaming/analytics/decay.py`](../streaming/analytics/decay.py)
- **Comparison:** 60s sliding window moving average vs Exponential decay intensity score.

### 2.8 Market-Basket Analysis: A-Priori & PCY
- **Concept:** Transaction mining over 1-second host communication baskets. PCY compresses candidate pairs during pass 1 using hash bucket bitmaps.
- **File:** [`streaming/analytics/itemsets.py`](../streaming/analytics/itemsets.py)
- **Comparison:** Candidate pairs generated by A-Priori vs PCY hash bucket filtered candidates ($> 40\%$ reduction).

### 2.9 Link Analysis: Graph Topology, Centrality, PageRank & Markov Model
- **Concept:** Directed multigraphs representing IP communications, Degree & Betweenness Centrality, Power-Iteration PageRank, and Markov transition anomaly scores ($-\log_2 P_{uv}$).
- **Files:** [`linkanalysis/graph.py`](../linkanalysis/graph.py), [`linkanalysis/centrality.py`](../linkanalysis/centrality.py), [`linkanalysis/pagerank.py`](../linkanalysis/pagerank.py), [`linkanalysis/markov.py`](../linkanalysis/markov.py)
- **Comparison:** Power iteration PageRank vs NetworkX PageRank ($\text{Diff} < 10^{-4}$).

---

## 3. How to Verify All Concepts

Run the automated validation and test suites:

```bash
# 1. Verify Environment & Prerequisites
python3 scripts/verify_env.py

# 2. Run Pure Algorithmic Unit Tests
pytest tests/unit/ -v

# 3. Run Cross-Validation Verification (Group A & Group B Consistency)
pytest tests/e2e/test_cross_validation.py -v

# 4. Run Multi-Window & 8-Scenario End-to-End Suite
pytest tests/e2e/test_multi_window.py tests/e2e/test_scenarios.py -v

# 5. Launch Full Pipeline Demo
bash scripts/run_demo.sh --mode replay --scenario normal
```
