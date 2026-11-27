// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef _INCSWITCH_H
#define _INCSWITCH_H

#include "inc_fat_tree_switch.h"
#include "incpacket.h"
#include <map>
#include <set>
#include <vector>

enum IncProtocolMode {
    INC_MODE_SWITCHML = 0,
    INC_MODE_ATP = 1
};

struct AggregationEntry {
    std::set<uint32_t> received_flows;
    simtime_picosec first_arrival = 0;
    uint32_t aggregated_data = 0;
    uint64_t worker_mask = 0;
    bool has_bypassed = false;
};

class IncSwitch : public IncFatTreeSwitch {
public:
    IncSwitch(EventList& eventlist, string s, switch_type t, uint32_t id, simtime_picosec delay, IncFatTreeTopology* ft);

    void receivePacket(Packet& pkt) override;

    bool hasUpLinks();
    void handle_inc_packet(Packet* p);
    void send_aggregated_packet(uint32_t job_id, uint32_t block_id, uint32_t aggregated_data, uint32_t pkt_size, uint64_t worker_mask = 0, bool is_bypassed = false);
    void send_multicast_down(Packet* p, uint32_t aggregated_data, uint32_t pkt_size, uint64_t worker_mask = 0, bool is_bypassed = false);

    // Multi-tenant: partecipanti e bitmap isolati per job_id
    static void add_job_participant(uint32_t host_id, uint32_t job_id);
    static std::map<uint32_t, std::vector<uint32_t>> _job_participants;
    static std::map<uint32_t, std::map<uint32_t, uint64_t>> _host_worker_masks;
    vector<int> get_multicast_ports(const vector<uint32_t>& participants);

    // Configurazione protocollo (SwitchML Statico vs ATP Dinamico) e telemetria
    static IncProtocolMode _inc_mode;
    static uint32_t _total_sram_slots;
    static uint64_t _sram_drops;
    static uint64_t _atp_bypassed_packets;

private:
    std::map<std::pair<uint32_t, uint32_t>, AggregationEntry> _aggregation_table;
    std::set<std::pair<uint32_t, uint32_t>> _completed_blocks;
    std::set<std::pair<uint32_t, uint32_t>> _bypassed_blocks;

    // SwitchML: slot usati per singolo Job | ATP: pool dinamico totale
    std::map<uint32_t, uint32_t> _used_slots_per_job;
    uint32_t _total_used_slots = 0;

    int calculate_expected_children(uint32_t job_id);
    uint64_t calculate_expected_mask(uint32_t job_id);
    bool is_destination_downstream(uint32_t dest_id);
    int select_best_port_towards_spine(uint32_t job_id, uint32_t block_id);
};

#endif