import sys

# Dimensione reale dei gradienti VGG16 in FP32 (~138.35M parametri * 4 byte)
VGG16_FULL_BYTES = 553430000

if len(sys.argv) < 3:
    print("Usage: python3 gen_vgg16_3jobs.py <filename> <total_nodes> [flowsize_bytes]")
    print("Example (VGG16 completo su 16 nodi): python3 gen_vgg16_3jobs.py vgg16_3jobs_16n.cm 16")
    sys.exit(1)

filename = sys.argv[1]
total_nodes = int(sys.argv[2])
flowsize = int(sys.argv[3]) if len(sys.argv) >= 4 else VGG16_FULL_BYTES

# Suddivisione dei nodi tra i 3 Job (es. su 16 nodi: Job1=6 nodi, Job2=5 nodi, Job3=5 nodi)
# Oppure su 32 nodi: Job1=12 nodi, Job2=10 nodi, Job3=10 nodi
n1 = total_nodes // 3 + (1 if total_nodes % 3 > 0 else 0)
n2 = total_nodes // 3 + (1 if total_nodes % 3 > 1 else 0)
n3 = total_nodes // 3

job_assignments = []
for host in range(0, n1):
    job_assignments.append((host, 1))
for host in range(n1, n1 + n2):
    job_assignments.append((host, 2))
for host in range(n1 + n2, n1 + n2 + n3):
    job_assignments.append((host, 3))

print(f"Generating VGG16 3-Job Multi-Tenant Workload:")
print(f"  File: {filename} | Total Nodes: {total_nodes} | Tensor Size: {flowsize} bytes ({flowsize/1e6:.2f} MB)")
print(f"  Job #1: {n1} workers (nodes 0..{n1-1})")
print(f"  Job #2: {n2} workers (nodes {n1}..{n1+n2-1})")
print(f"  Job #3: {n3} workers (nodes {n1+n2}..{total_nodes-1})")

with open(filename, "w") as f:
    print(f"Nodes {total_nodes}", file=f)
    print(f"Connections {len(job_assignments)}", file=f)
    print("Triggers 0", file=f)

    for idx, (src, job_id) in enumerate(job_assignments):
        flow_id = idx + 1
        print(f"{src}->#{job_id} id {flow_id} start 0 size {flowsize}", file=f)
