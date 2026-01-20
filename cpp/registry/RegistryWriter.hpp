// Copyright 2025 XTX Markets Technologies Limited
//
// SPDX-License-Identifier: GPL-2.0-or-later

#pragma once

#include "Loop.hpp"
#include "RegistryDB.hpp"
#include "RegistryDBLogEntry.hpp"
#include "RegistryServer.hpp"
#include "SPSC.hpp"
#include "LogsDB.hpp"

#include <vector>

class RegistryWriter : public Loop {
public:
    RegistryWriter(
        Logger &logger,
        std::shared_ptr<XmonAgent> xmon,
        RegistryDB& registryDB,
        RegistryServer& server,
        LogsDB& logsDB);

    virtual ~RegistryWriter() {}

    // Push methods for adding requests to the queue
    uint32_t pushLogsDBRequests(std::vector<LogsDBRequest>& requests) {
        return _logsDBRequestsQueue.push(requests);
    }
    uint32_t pushLogsDBResponses(std::vector<LogsDBResponse>& responses) {
        return _logsDBResponsesQueue.push(responses);
    }
    typename std::vector<RegistryRequest>::iterator pushRegistryRequests(
        typename std::vector<RegistryRequest>::iterator being,
        typename std::vector<RegistryRequest>::iterator end)
    {
        return _registryRequestsQueue.push(being, end);
    }

    uint32_t pushBlockServiceStateRequests(std::vector<RegistryRequest>& requests) {
        return _blockServiceStateRequestsQueue.push(requests);
    }

    void sendStop() override;

    virtual void step() override;

private:
    RegistryDB& _registryDB;
    RegistryServer& _server;
    LogsDB& _logsDB;

    MultiSPSCWaiter _waiter;

    SPSC<LogsDBRequest, true> _logsDBRequestsQueue;
    SPSC<LogsDBResponse, true> _logsDBResponsesQueue;
    SPSC<RegistryRequest, true> _registryRequestsQueue;
    SPSC<RegistryRequest, true> _blockServiceStateRequestsQueue;

    // Buffers for processing
    std::vector<LogsDBRequest> _logsDBRequests;
    std::vector<LogsDBResponse> _logsDBResponses;
    std::vector<RegistryRequest> _registryRequests;
    std::vector<RegistryResponse> _registryResponses;

    // Buffers for outgoing messages
    std::vector<LogsDBRequest*> _logsDBOutRequests;
    std::vector<LogsDBResponse> _logsDBOutResponses;

    // Buffer for RegistryDB processLogEntries result
    std::vector<RegistryDBWriteResult> _writeResults;

    // Buffer for log entries
    std::vector<LogsDBLogEntry> _logEntries;
    std::vector<EntryReqIdx> _entriesRequestIds;

    // Keeping track which log entry corresponds to which request
    std::unordered_map<EntryReqIdx, uint64_t> _logIdxToRequestId;

    void _applyLogEntries();
};
