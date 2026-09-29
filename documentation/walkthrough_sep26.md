# Walkthrough — September 26, 2026 (Day 6)

## Team Progress

### M A Sushil Kumar
- **T4-009: Counting ones — DGIM sliding-window estimator**
  - Implemented Datar–Gionis–Indyk–Motwani (DGIM) algorithm in `streaming/analytics/dgim.py`
  - Maintains exponential bucket sizes (powers of 2) with at most 2 buckets of any size
  - Estimates number of 1-bits (e.g. TCP SYN flags or high-traffic flag triggers) in a sliding window of size N
  - Writes results to `counting_ones` table in serving DB
  - Unit tests in `tests/unit/test_dgim.py` verify 50% max relative error bound guarantee

### Yashwant Vadhan M
- **T5-004: Market-basket model and limited-pass frequent itemsets (A-Priori + PCY)**
  - Implemented A-Priori and Park-Chen-Yu (PCY) algorithms in `streaming/analytics/itemsets.py`
  - PCY uses hash buckets for candidate pairs during Pass 1 to reduce candidate size in Pass 2
  - Finds frequent protocol and port co-occurrences in streaming traffic windows
  - Results written to `frequent_itemsets` serving table
  - Unit tests in `tests/unit/test_itemsets.py` assert candidate counting and hash bucket filtering

### Priyan S
- **T6-006 & T7-006: Link Analysis test pack & Link Analysis tab**
  - Integrated 3D Cyberpunk network graph renderer (`dashboard/components/graph3d.py`) using Plotly
  - Implemented PageRank table and Markov next-hop transition matrix visualizer
  - Unit tests in `tests/unit/test_linkanalysis.py` verify graph algorithms against toy network topology

## Key Decisions
- Standardized hash function seed for PCY candidate pair hashing to guarantee deterministic bucket indexing
- Set DGIM default window size N=1000 items

## Files Changed
- `streaming/analytics/dgim.py` — DGIM algorithm
- `streaming/analytics/itemsets.py` — A-Priori & PCY algorithms
- `tests/unit/test_dgim.py` — DGIM tests
- `tests/unit/test_itemsets.py` — Itemsets tests
- `dashboard/components/graph3d.py` — 3D graph visualizer
- `documentation/walkthrough_sep26.md` — Daily walkthrough
