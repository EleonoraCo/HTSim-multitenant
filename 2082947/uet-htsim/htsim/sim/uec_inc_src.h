// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#ifndef UEC_INC_SRC_H
#define UEC_INC_SRC_H

#include "uec.h"
#include <map>
#include <set>

class UecIncNIC : public UecNIC {
public:
    UecIncNIC(id_t src_num, EventList& eventList, linkspeed_bps linkspeed, uint32_t ports);

    // Override of the end-of-transmission event to inject INC priority
    void doNextEvent() override;
};

class UecIncSrc : public UecSrc {
public:
    UecIncSrc(TrafficLogger* trafficLogger,
              EventList& eventList,
              unique_ptr<UecMultipath> mp,
              UecNIC& nic,
              uint32_t no_of_ports,
              bool rts = false);

    // Override of this method to have INC logic
    virtual void receivePacket(Packet& pkt, uint32_t portnum) override;
    virtual mem_b sendNewPacket(const Route& route) override;
    virtual mem_b sendRtxPacket(const Route& route) override;

    // Inizialization method for INC
    void initIncNscc(mem_b cwnd, simtime_picosec peer_rtt);

// Setter per il Job ID Multi-Tenant e ATP Worker Bitmap
    void setIncJobId(uint32_t job_id) { _inc_job_id = job_id; }
    void setAtpMode(bool atp) { _atp_mode = atp; }
    void setIncWorkerMask(uint64_t mask) { _inc_worker_mask = mask; }
    void setIncFullJobMask(uint64_t full_mask) { _inc_full_job_mask = full_mask; }

private:
    uint32_t _inc_job_id = 1;
    bool _atp_mode = false;
    uint64_t _inc_worker_mask = 0;
    uint64_t _inc_full_job_mask = 0;

    // ATP Host Fallback: accumula maschere parziali per blocchi bypassati dagli switch
    std::map<uint32_t, uint64_t> _atp_partial_masks;
    std::set<uint32_t> _atp_acked_blocks;
    uint64_t _atp_contiguous_block = 0;
};

#endif