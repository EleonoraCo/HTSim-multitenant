// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef _INCSWITCH_H
#define _INCSWITCH_H

#include "inc_fat_tree_switch.h"
#include "incpacket.h"
#include <map>
#include <set>
#include <vector>

// Structure to trace the state of aggregation
struct AggregationEntry {
    std::set<uint32_t> received_flows;
    simtime_picosec first_arrival;
    uint32_t aggregated_data = 0;
};

class IncSwitch : public IncFatTreeSwitch {
public:
    IncSwitch(EventList& eventlist, string s, switch_type t, uint32_t id, simtime_picosec delay, IncFatTreeTopology* ft);

    // Override of receiving method
    void receivePacket(Packet& pkt) override;

    // Method for INC
    bool hasUpLinks();
    void handle_inc_packet(Packet* p);
    void send_aggregated_packet(uint32_t job_id, uint32_t block_id, uint32_t aggregated_data, uint32_t pkt_size);
    void send_multicast_down(Packet* p, uint32_t aggregated_data, uint32_t pkt_size);

    // Multi-tenant: partecipanti isolati per job_id
    static void add_job_participant(uint32_t host_id, uint32_t job_id);
    static std::map<uint32_t, std::vector<uint32_t>> _job_participants;
    vector<int> get_multicast_ports(const vector<uint32_t>& participants);

    // SwitchML Statico: capacita' totale SRAM dello switch (numero massimo di blocchi aggregabili contemporaneamente)
    static uint32_t _total_sram_slots;
    static uint64_t _sram_drops;

private:
    // State tables specific to this switch instance
    std::map<std::pair<uint32_t, uint32_t>, AggregationEntry> _aggregation_table;
    std::set<std::pair<uint32_t, uint32_t>> _completed_blocks;

    // Contatore degli slot SRAM attualmente occupati da ciascun Job in questo switch
    std::map<uint32_t, uint32_t> _used_slots_per_job;

// Internal helper (ora dipendono dal job_id)
    int calculate_expected_children(uint32_t job_id);
    bool is_destination_downstream(uint32_t dest_id);
    int select_best_port_towards_spine(uint32_t job_id, uint32_t block_id);
};

#endif