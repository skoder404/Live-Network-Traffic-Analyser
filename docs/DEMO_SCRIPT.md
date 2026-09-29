# LNTA Demo Script & Rehearsal Guide

## Roles
- **Presenter 1 (Architecture & Ingestion)**: Explains the big picture, Flume, and HDFS.
- **Presenter 2 (Streaming & Algorithms)**: Explains Spark processing, algorithms used, and data transformations.
- **Presenter 3 (Dashboard & UI)**: Drives the screen sharing, explains dashboard components, and live metrics.
- **Presenter 4 (Q&A Lead)**: Handles incoming questions, tests, and backup scenarios.

## Timing Guide (10-15 minutes total)
- **0:00 - 2:00**: Intro & Architecture Diagram
- **2:00 - 4:00**: Start Pipeline & Show Ingestion (Flume)
- **4:00 - 8:00**: Live Dashboard (Tabs one by one)
- **8:00 - 11:00**: Streaming Algorithms & Concept Mapping
- **11:00 - 12:00**: Testing & Code Quality
- **12:00 - 15:00**: Q&A

## Step-by-step Demo Flow

1. **Show Architecture Diagram (2 mins)**
   - Display the overall flow from Network -> Capture -> Flume -> Spark -> Dashboard.
2. **Start Pipeline (run_demo.sh) (2 mins)**
   - `bash scripts/run_demo.sh`
   - Show logs briefly to prove data is moving.
3. **Show Live Dashboard Tabs (4 mins)**
   - **Live**: Overall throughput (PPS/BPS).
   - **Stream concepts**: Explain how items update.
   - **Link analysis**: Show the top edges and nodes.
   - **Alerts**: Point out simulated attacks/port scans.
4. **Explain Algorithms with Concept Mapping (3 mins)**
   - Tie UI elements to BDMS concepts (e.g., Bloom filters, Flajolet-Martin, Reservior Sampling).
5. **Show Test Results (1 min)**
   - Run a quick `pytest` to show the robust test suite.
6. **Q&A Preparation**
   - Be ready to discuss "Why not Kafka?" or "Why SQLite for serving?".

## Rehearsal Checklists

### Rehearsal 1 (Flow & Timing)
- [ ] Presenters know their handoffs.
- [ ] Total time is under 15 minutes.
- [ ] Screen sharing transitions are smooth.

### Rehearsal 2 (Technical Dry-Run)
- [ ] Pipeline starts cleanly using `run_demo.sh`.
- [ ] Data populates the dashboard within 10 seconds.
- [ ] Alerts trigger successfully during the demo window.

### Rehearsal 3 (Failure Simulation)
- [ ] Presenters can quickly switch to offline fallback if needed.
- [ ] Team practices answering 3 anticipated hard questions.

## Backup Plan
If the live pipeline fails or cluster goes down:
1. Immediately switch to the offline snapshot (`LNTA_MOCK=true streamlit run dashboard/app.py`).
2. Run the pipeline with `--local` flags to bypass HDFS/Spark cluster issues.
3. Use pre-recorded screenshots (have a slide deck ready as a last resort).
