# UEC htsim — In-Network Computing (INC) Multi-Tenant Extension

`uet-htsim` is a high-performance, discrete-event network simulator based on the open-source **htsim** simulator, extended to model Ultra Ethernet Consortium (UEC) transport mechanisms (NSCC, packet trimming, multipath spraying) and **In-Network Computing (INC)** collectives (All-Reduce) for distributed Deep Neural Network (DNN) training.

## Project Status & Architecture Overview

This repository extends the single-tenant UEC INC baseline with multi-tenant capabilities for programmable switches:

1. **Static Multi-Tenant SwitchML (Implemented):**
   - **Per-Job Isolation:** Programmable switches (`IncSwitch`) maintain independent reduction trees, expected child counts, and multicast downlink fan-outs indexed by `job_id`.
   - **Deterministic Core Convergence:** Uplink port selection toward Aggregation and Core tiers uses a deterministic hash on `(job_id, block_id)` so that all branches of the same collective block meet at the exact same spine switch regardless of asymmetric queue loads across shared pods.
   - **Static SRAM Partitioning:** Each switch divides its total aggregation memory (`-sram_slots`) into fixed, exclusive partitions ($\lfloor \text{total\_sram\_slots} / \text{num\_jobs} \rfloor$). Packets arriving when a job's static partition is full are dropped (`_sram_drops`) and recovered via host RTO retransmissions.
   - **RTO State Consistency:** Fixed `UecIncSrc::sendRtxPacket` to properly register retransmitted packets via `createSendRecord()`, preventing `_send_times` assertion failures under heavy congestion and SRAM saturation.
2. **Dynamic Multi-Tenant ATP (Next Milestone):**
   - Shared dynamic SRAM pooling (first-come, first-served) and best-effort unaggregated bypass fallback to eliminate SRAM drops and multi-tier head-of-line blocking.

---

## Repository Layout

- `sim/` — Core simulator engine, UEC transport (`uec.h/.cpp`), and INC host source/NIC logic (`uec_inc_src.h/.cpp`, `incpacket.h`).
- `sim/datacenter/` — Data center topologies, programmable switch models (`inc_switch.h/.cpp`), multi-tenant traffic matrix parser (`inc_connection_matrix.h/.cpp`), simulation entry point (`main_inc_uec.cpp`), and compiled binaries (`htsim_inc_uec`, `htsim_uec`).
- `sim/datacenter/connection_matrices/` — Traffic matrices (`.cm`) and Python workload generators, including `gen_allreduce_ina.py` (single-tenant) and `gen_vgg16_3jobs.py` (3-job multi-tenant VGG16).
- `sim/datacenter/topologies/` — Predefined Fat-Tree topology files.
- `sim/datacenter/validation/` — Upstream UEC validation suite.

---

## Getting Started & Building

### Prerequisites
- C++17 compatible compiler (`g++` or `clang++`) and `cmake` ($\ge 3.16$)
- Python 3 (for generating workload connection matrices)

### Build Instructions

From the `htsim/sim` directory, configure and compile the project:

```bash
cd ~/2082947/uet-htsim/htsim/sim
cmake -S . -B build
cmake --build build --parallel
```

To recompile only the INC executable after modifying source files:

```bash
cd ~/2082947/uet-htsim/htsim/sim/build
make htsim_inc_uec -j4
```

The build system automatically creates/updates the executable symlink at `sim/datacenter/htsim_inc_uec`.

---

## Multi-Tenant Traffic Matrix Format (`.cm`)

In-Network Computing All-Reduce flows are declared in `.cm` files using the `#<job_id>` destination syntax:

```text
Nodes <total_nodes>
Connections <total_connections>
Triggers 0
<src_node>-><#job_id> id <flow_id> start <start_time_us> size <bytes>
```

- `#<job_id>` assigns the worker to a specific training job (e.g., `#1`, `#2`, `#3`). A plain `#` without an integer defaults to `job_id = 1` for backward compatibility.
- `start` is expressed in **microseconds (`us`)** (e.g., `start 0` for synchronous start, `start 30` to model a $30\ \mu\text{s}$ straggler skew).
- `size` is the gradient tensor size in **bytes** transmitted by each worker.

---

## Running Multi-Tenant SwitchML Experiments

All simulation commands below must be executed from `sim/datacenter`:

```bash
cd ~/2082947/uet-htsim/htsim/sim/datacenter
```

### Key CLI Parameters for `htsim_inc_uec`

| Flag | Description | Example |
| :--- | :--- | :--- |
| `-nodes <N>` | Number of host nodes in the Fat-Tree topology | `-nodes 16` |
| `-strat <mode>` | Multipath routing strategy | `-strat ecmp` |
| `-tm <file.cm>` | Path to the traffic/connection matrix file | `-tm vgg16_3jobs_16n.cm` |
| `-sram_slots <K>` | Total SwitchML SRAM aggregation slots per switch (statically split across active jobs) | `-sram_slots 1024` |
| `-end <us>` | Maximum simulation duration in microseconds | `-end 1000000` |

---

### 1. Functional Sanity Test (2 Asymmetric Jobs)

Verifies logical isolation, independent reduction trees, and synchronized completion across two concurrent jobs of different sizes (`100 KB` for Job `#1` on nodes `0..7`, `200 KB` for Job `#2` on nodes `8..15`):

```bash
cat << 'EOF' > test_multitenant.cm
Nodes 16
Connections 16
Triggers 0
0->#1 id 1 start 0 size 100000
1->#1 id 2 start 0 size 100000
2->#1 id 3 start 0 size 100000
3->#1 id 4 start 0 size 100000
4->#1 id 5 start 0 size 100000
5->#1 id 6 start 0 size 100000
6->#1 id 7 start 0 size 100000
7->#1 id 8 start 0 size 100000
8->#2 id 9 start 0 size 200000
9->#2 id 10 start 0 size 200000
10->#2 id 11 start 0 size 200000
11->#2 id 12 start 0 size 200000
12->#2 id 13 start 0 size 200000
13->#2 id 14 start 0 size 200000
14->#2 id 15 start 0 size 200000
15->#2 id 16 start 0 size 200000
EOF

./htsim_inc_uec -nodes 16 -strat ecmp -tm test_multitenant.cm -sram_slots 1024 -end 100000
```

**Expected behavior:** Job `#1` completes at `10.124 us` (`25` packets/worker) and Job `#2` completes at `18.9109 us` (`49` packets/worker) with `Rtx: 0` and `SRAM Static Drops: 0`.

---

### 2. VGG16 3-Job Multi-Tenant Benchmark

Models three concurrent distributed training jobs running **VGG16** ($\approx 138.35\text{M}$ parameters, **$553,430,000\text{ bytes}$ / $553.43\text{ MB}$** gradient tensor in FP32 per worker) on a 16-node Fat-Tree ($k=4$, $100\text{ Gbps}$ links, $4150\text{ B}$ MTU):
- **Job `#1`:** 6 workers (nodes `0..5`)
- **Job `#2`:** 5 workers (nodes `6..10`)
- **Job `#3`:** 5 workers (nodes `11..15`)

#### Step A: Generate the Synchronous Workload (`start 0`)

```bash
python3 connection_matrices/gen_vgg16_3jobs.py vgg16_3jobs_16n.cm 16
```

Run with ample SRAM (`1024` total slots $\rightarrow 341$ slots/job) and constrained SRAM (`48` total slots $\rightarrow 16$ slots/job):

```bash
# Ample SRAM (baseline line-rate performance)
./htsim_inc_uec -nodes 16 -strat ecmp -tm vgg16_3jobs_16n.cm -sram_slots 1024 -end 500000

# Constrained SRAM (synchronous start)
./htsim_inc_uec -nodes 16 -strat ecmp -tm vgg16_3jobs_16n.cm -sram_slots 48 -end 500000
```

#### Step B: Generate & Run the Straggler-Skew Workload (`30 us` jitter)

To evaluate SwitchML static partitioning under realistic computation jitter (where one worker per job starts $30\ \mu\text{s}$ later than its peers, causing in-flight blocks to accumulate in switch SRAM):

```bash
cp vgg16_3jobs_16n.cm vgg16_3jobs_16n_skew.cm
sed -i 's/^5->#1 id 6 start 0/5->#1 id 6 start 30/' vgg16_3jobs_16n_skew.cm
sed -i 's/^10->#2 id 11 start 0/10->#2 id 11 start 30/' vgg16_3jobs_16n_skew.cm
sed -i 's/^15->#3 id 16 start 0/15->#3 id 16 start 30/' vgg16_3jobs_16n_skew.cm

# 1. Ample SRAM (1024 slots -> 341 static slots/job): absorbs the 30 us skew cleanly
./htsim_inc_uec -nodes 16 -strat ecmp -tm vgg16_3jobs_16n_skew.cm -sram_slots 1024 -end 1000000

# 2. Constrained SRAM (48 slots -> 16 static slots/job): saturates static slices and triggers SRAM drops
./htsim_inc_uec -nodes 16 -strat ecmp -tm vgg16_3jobs_16n_skew.cm -sram_slots 48 -end 1000000
```

---

### Understanding Simulation Output

At the end of each run, `htsim_inc_uec` prints per-flow completion times and global transport/switch counters:

```text
Flow Uec_0_-1 flowId 1 UecIncSrc 0 finished at 45000.4 total messages 1 total packets 135446 RTS 0 total bytes 553432356 ...
.Done
New: 2167136 Rtx: 0 RTS: 0 Bounced: 0 ACKs: 2167136 NACKs: 0 Pulls: 0 sleek_pkts: 0
SwitchML SRAM total slots: 1024 | SRAM Static Drops: 0
```

- `finished at <time>`: Job Completion Time (JCT) for that worker in **microseconds (`us`)**.
- `New` / `Rtx` / `ACKs`: Total original packets transmitted, RTO/fast retransmissions, and completed All-Reduce block acknowledgments across the network.
- `SwitchML SRAM total slots` / `SRAM Static Drops`: Configured per-switch SRAM capacity and total packets dropped across all switches due to static per-job partition exhaustion.

---

## Upstream UEC Validation & Unit Tests

### Standard UEC Transport Validation
From `sim/datacenter/validation`, the upstream UEC congestion control validation suite can be run via:

```bash
cd ~/2082947/uet-htsim/htsim/sim/datacenter/validation
python3 validate_all.py --config_json_file example_validate.json > out.txt
```

### Unit Tests (GoogleTest)
To compile and execute the unit test suite from `sim/`:

```bash
cd ~/2082947/uet-htsim/htsim/sim
cmake -S . -B build -DENABLE_TESTS=ON
cmake --build build --parallel
cd build && ctest
```