// Copyright 2025 XTX Markets Technologies Limited
//
// SPDX-License-Identifier: GPL-2.0-or-later

#include "RegistryDBLogEntry.hpp"
#include "Assert.hpp"
#include "Bincode.hpp"
#include "LogsDB.hpp"
#include "MsgsGen.hpp"
#include "RegistryServer.hpp"


RegistryDBLogEntry::RegistryDBLogEntry() { clear(); }

size_t RegistryDBLogEntry::packedSize() const {
    return requests.packedSize() + sizeof(entryTime);
}

void RegistryDBLogEntry::pack(BincodeBuf& buf) const {
    buf.packScalar(entryTime.ns);
    buf.packList(requests);
}

void RegistryDBLogEntry::unpack(BincodeBuf& buf) {
    entryTime.ns = buf.unpackScalar<uint64_t>();
    buf.unpackList(requests);
}

void RegistryDBLogEntry::clear() {
    entryTime = TernTime(0);
    requests.clear();
}

bool RegistryDBLogEntry::operator==(const RegistryDBLogEntry&rhs) const {
    return entryTime == rhs.entryTime && requests == rhs.requests;
}

void RegistryDBLogEntry::requestsToLogEntries(std::vector<RegistryRequest>& requests,
    size_t maxLogEntrySize, TernTime entryTime, std::vector<LogsDBLogEntry>& out, std::vector<EntryReqIdx>& outIndices)
{
    RegistryDBLogEntry currentEntry;
    currentEntry.entryTime = entryTime;
    size_t sizeAvailable = maxLogEntrySize - currentEntry.packedSize();
    EntryReqIdx currentIdx{out.size(), 0};
    auto serialize = [&]() {
        auto& newEntry = out.emplace_back();
        newEntry.value.resize(maxLogEntrySize - sizeAvailable);
        BincodeBuf buf((char*)newEntry.value.data(), newEntry.value.size());
        currentEntry.pack(buf);
        buf.ensureFinished();
        currentEntry.clear();
        currentEntry.entryTime = entryTime;
        sizeAvailable = maxLogEntrySize - currentEntry.packedSize();
        ++currentIdx.logIdx;
        currentIdx.offset = 0;
    };
    for (auto &reqWrapper : requests) {
        auto &req = reqWrapper.req;
        if (req.packedSize() > sizeAvailable) {
            serialize();
        }
        auto reqPackedSize = req.packedSize();
        if (reqPackedSize > sizeAvailable) {
            // This is the only request too big to fit in an empty log entry. We need to split it.
            ALWAYS_ASSERT(req.kind() == RegistryMessageKind::REGISTER_BLOCK_SERVICES);
            auto outBlockServices = &currentEntry.requests.els.emplace_back().setRegisterBlockServices().blockServices.els;
            sizeAvailable -= currentEntry.requests.els.back().packedSize();
            auto& blockServices = req.getRegisterBlockServices().blockServices.els;
            for (auto& blockService : blockServices) {
                auto bsPackedSize = blockService.packedSize();
                if (bsPackedSize > sizeAvailable) {
                    serialize();
                    outBlockServices = &currentEntry.requests.els.emplace_back().setRegisterBlockServices().blockServices.els;
                    sizeAvailable -= currentEntry.requests.els.back().packedSize();
                }
                outBlockServices->push_back(std::move(blockService));
                sizeAvailable -= bsPackedSize;
            }
            outIndices.push_back(currentIdx);
            ++currentIdx.offset;
            continue;
        }
        currentEntry.requests.els.emplace_back(std::move(req));
        sizeAvailable -= reqPackedSize;
        outIndices.push_back(currentIdx);
        ++currentIdx.offset;
    }
    if (!currentEntry.requests.els.empty()) {
        serialize();
    }
}
