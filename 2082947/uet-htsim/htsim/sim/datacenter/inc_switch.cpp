// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "inc_switch.h"
#include "network.h"
#include "inc_fat_tree_topology.h"
#include "uec.h"
#include <iostream>

std::map<uint32_t, std::vector<uint32_t>> IncSwitch::_job_participants;
uint32_t IncSwitch::_total_sram_slots = 1024; // Default: 1024 slot totali nello switch
uint64_t IncSwitch::_sram_drops = 0;

IncSwitch::IncSwitch(EventList& eventlist, string s, switch_type t, uint32_t id, simtime_picosec delay, IncFatTreeTopology* ft)
    : IncFatTreeSwitch(eventlist, s, t, id, delay, ft) {
}

void IncSwitch::add_job_participant(uint32_t host_id, uint32_t job_id) {
    std::vector<uint32_t>& list = _job_participants[job_id];
    for (uint32_t existing : list) {
        if (existing == host_id) return;
    }
    list.push_back(host_id);
}

void IncSwitch::receivePacket(Packet& pkt) {
    if (pkt.type() == INC_RESULT && pkt.get_direction() == DOWN) {
        IncPacket* p = (IncPacket*)&pkt;
        send_multicast_down(&pkt, p->_inc_int_data, p->size());
        pkt.free();
        return;
    }

    UecBasePacket* uec_pkt = dynamic_cast<UecBasePacket*>(&pkt);
    if (uec_pkt && uec_pkt->dst() == UINT32_MAX) {
        pkt._is_inc = true;
    }

    if (pkt._is_inc) {
        handle_inc_packet(&pkt);
        return;
    }

    IncFatTreeSwitch::receivePacket(pkt);
}

bool IncSwitch::hasUpLinks() {
    if (_type == CORE) {
        return false;
    }
    if (_type == TOR) {
        return true;
    }
    if (_type == AGG) {
        return (_ft->cfg().get_tiers() > 2);
    }
    return false;
}

void IncSwitch::handle_inc_packet(Packet* p) {
    uint32_t job_id = p->_inc_job_id;
    uint32_t block_id = p->_inc_block_id;
    uint32_t contributor_id = (p->_inc_last_switch_id != -1) ? (uint32_t)p->_inc_last_switch_id : p->flow_id();

    auto key = std::make_pair(job_id, block_id);

    // 1. Scarta pacchetti fantasma di blocchi gia' completati
    if (_completed_blocks.find(key) != _completed_blocks.end()) {
        p->free();
        return;
    }

    // 2. Se il blocco non e' ancora in tabella, applica il partizionamento statico SwitchML
    if (_aggregation_table.find(key) == _aggregation_table.end()) {
        uint32_t num_jobs = _job_participants.size();
        uint32_t max_slots_per_job = (num_jobs > 0) ? (_total_sram_slots / num_jobs) : _total_sram_slots;
        if (max_slots_per_job == 0) max_slots_per_job = 1; // Garanzia minima di 1 slot

        if (_used_slots_per_job[job_id] >= max_slots_per_job) {
         // La fetta statica di memoria SRAM assegnata a questo Job e' piena!
         _sram_drops++;
         p->free();
         return;
         }

        // Alloca la nuova entry e occupa uno slot nella partizione del Job
        AggregationEntry entry;
        entry.first_arrival = eventlist().now();
        entry.aggregated_data = 0;
        _aggregation_table[key] = entry;
        _used_slots_per_job[job_id]++;
    }

    // 3. Aggiorna somma e set dei contributori (idempotenza)
    if (_aggregation_table[key].received_flows.find(contributor_id) == _aggregation_table[key].received_flows.end()) {
        _aggregation_table[key].aggregated_data += p->_inc_int_data;
        _aggregation_table[key].received_flows.insert(contributor_id);
    }

    int expected_children = calculate_expected_children(job_id);

    // 4. Verifica completamento dell'aggregazione per questo switch
    if (expected_children > 0 && _aggregation_table[key].received_flows.size() >= (uint32_t)expected_children) {
        uint32_t final_sum = _aggregation_table[key].aggregated_data;

        _completed_blocks.insert(key);
        _aggregation_table.erase(key);

        // Libera lo slot SRAM della partizione statica del Job
        if (_used_slots_per_job[job_id] > 0) {
            _used_slots_per_job[job_id]--;
        }

        // Controlla se questo switch copre tutti i partecipanti di QUESTO specifico job_id
        bool i_cover_all_participants = true;
        const std::vector<uint32_t>& participants = _job_participants[job_id];
        for (uint32_t participant_id : participants) {
            if (!is_destination_downstream(participant_id)) {
                i_cover_all_participants = false;
                break;
            }
        }

        if (i_cover_all_participants) {
            send_multicast_down(p, final_sum, p->size());
        } else if (hasUpLinks()) {
            send_aggregated_packet(job_id, block_id, final_sum, p->size());
        }
    }

    p->free();
}

void IncSwitch::send_aggregated_packet(uint32_t job_id, uint32_t block_id, uint32_t aggregated_data, uint32_t pkt_size) {
    int best_port = select_best_port_towards_spine(job_id, block_id);
    if (best_port == -1) return;

    BaseQueue* q = _ports.at(best_port);
    PacketSink* next_hop_sink = q->getRemoteEndpoint();
    if (!next_hop_sink) return;

    Route* route = new Route();
    route->push_back(next_hop_sink);

    IncPacket* p = IncPacket::newpkt(*route, job_id, block_id, aggregated_data, INC_DATA, pkt_size);

    p->_inc_last_switch_id = getID();
    p->set_next_hop(next_hop_sink);
    p->set_direction(UP);

    q->receivePacket(*p);
}

void IncSwitch::send_multicast_down(Packet* p, uint32_t aggregated_data, uint32_t pkt_size) {
    uint32_t job_id = p->_inc_job_id;
    vector<int> target_ports = get_multicast_ports(_job_participants[job_id]);

    for (int port_index : target_ports) {
        BaseQueue* q = _ports.at(port_index);
        PacketSink* next_hop_sink = q->getRemoteEndpoint();

        Route* hop_route = new Route();
        hop_route->push_back(next_hop_sink);

        IncPacket* copy = IncPacket::newpkt(*hop_route, p->_inc_job_id, p->_inc_block_id, aggregated_data, INC_DATA, pkt_size);
        copy->make_result();
        copy->set_direction(DOWN);
        copy->set_next_hop(next_hop_sink);
        copy->_inc_last_switch_id = getID();

        q->receivePacket(*copy);
    }
}

int IncSwitch::calculate_expected_children(uint32_t job_id) {
    auto it = _job_participants.find(job_id);
    if (it == _job_participants.end() || it->second.empty()) return 0;

    const std::vector<uint32_t>& participants = it->second;
    uint32_t type = getType();

    if (type == TOR) {
        int expected = 0;
        for (uint32_t host_id : participants) {
            if (_ft->cfg().HOST_POD_SWITCH(host_id) == getID()) expected++;
        }
        return expected;
    } else if (type == AGG) {
        std::set<uint32_t> active_tors;
        uint32_t my_pod = _ft->cfg().AGG_SWITCH_POD_ID(getID());
        for (uint32_t host_id : participants) {
            if (_ft->cfg().HOST_POD(host_id) == my_pod)
                active_tors.insert(_ft->cfg().HOST_POD_SWITCH(host_id));
        }
        return active_tors.size();
    } else if (type == CORE) {
        std::set<uint32_t> active_pods;
        for (uint32_t host_id : participants)
            active_pods.insert(_ft->cfg().HOST_POD(host_id));
        return active_pods.size();
    }
    return 0;
}

bool IncSwitch::is_destination_downstream(uint32_t dest_id) {
    uint32_t type = getType();
    if (type == TOR) return _ft->cfg().HOST_POD_SWITCH(dest_id) == getID();
    if (type == AGG) return _ft->cfg().HOST_POD(dest_id) == _ft->cfg().AGG_SWITCH_POD_ID(getID());
    if (type == CORE) return true;
    return false;
}

vector<int> IncSwitch::get_multicast_ports(const vector<uint32_t>& participants) {
    set<int> unique_ports;
    uint32_t type = getType();

    for (uint32_t dest_id : participants) {
        if (!is_destination_downstream(dest_id)) continue;

        int port_index = -1;
        if (type == TOR) {
            BaseQueue* q = _ft->queues_nlp_ns[getID()][dest_id][0];
            for (size_t i = 0; i < _ports.size(); ++i) {
                if (_ports[i] == q) { port_index = i; break; }
            }
        } else if (type == AGG) {
            uint32_t target_tor = _ft->cfg().HOST_POD_SWITCH(dest_id);
            BaseQueue* q = _ft->queues_nup_nlp[getID()][target_tor][0];
            for (size_t i = 0; i < _ports.size(); ++i) {
                if (_ports[i] == q) { port_index = i; break; }
            }
        } else if (type == CORE) {
            uint32_t pod_id = _ft->cfg().HOST_POD(dest_id);
            uint32_t agg_target = _ft->cfg().MIN_POD_AGG_SWITCH(pod_id) + (getID() % _ft->cfg().agg_switches_per_pod());
            BaseQueue* q = _ft->queues_nc_nup[getID()][agg_target][0];
            for (size_t i = 0; i < _ports.size(); ++i) {
                if (_ports[i] == q) { port_index = i; break; }
            }
        }

        if (port_index != -1) unique_ports.insert(port_index);
    }
    return vector<int>(unique_ports.begin(), unique_ports.end());
}

int IncSwitch::select_best_port_towards_spine(uint32_t job_id, uint32_t block_id) {
    size_t start_up_port = 0;
    uint32_t type = getType();

    if (type == TOR) start_up_port = _ft->cfg().radix_down(TOR_TIER);
    else if (type == AGG) start_up_port = _ft->cfg().radix_down(AGG_TIER);
    else return -1;

    std::vector<int> valid_up_ports;
    for (size_t i = start_up_port; i < _ports.size(); i++) {
        if (_ports.at(i)->getRemoteEndpoint() != NULL) {
            valid_up_ports.push_back(i);
        }
    }

    if (valid_up_ports.empty()) return -1;

    // Hash deterministico su (job_id, block_id): garantisce che tutti i Pod
    // convergano sullo stesso switch Aggregation e sullo stesso switch Core
    uint64_t hash = (static_cast<uint64_t>(job_id) * 2654435761ULL) ^ static_cast<uint64_t>(block_id);
    return valid_up_ports[hash % valid_up_ports.size()];
}