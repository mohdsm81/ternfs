// Copyright 2025 XTX Markets Technologies Limited
//
// SPDX-License-Identifier: GPL-2.0-or-later

#include "BlockServiceFileTracker.hpp"
#include "Assert.hpp"
#include "Env.hpp"
#include "MsgsGen.hpp"

BlockServiceFileTracker::BlockServiceFileTracker(Logger& logger, std::shared_ptr<XmonAgent>& xmon, const RegistryOptions& options, RegistryDB& db, RegistryServer& server, RegistryWriter& writer) :
    Loop(logger, xmon, "file_tracker"),
    _registryDB(db),
    _server(server),
    _writer(writer),
    _queue(MAX_SHARD_RESPONSES),
    _nextRequestId(RandomGenerator().generate64())
{
    _responses.reserve(MAX_SHARD_RESPONSES);
}

void BlockServiceFileTracker::step() {
    _processResponses();
    _scanBlockServices();
    _sendRequests();
}

void BlockServiceFileTracker::pushShardResponses(std::vector<ShardResponse>& responses) {
    _queue.push(responses);
}

void BlockServiceFileTracker::_processResponses() {
    _queue.pull(_responses, MAX_SHARD_RESPONSES, 100_ms);
    std::vector<RegistryRequest> reqs;
    for (const auto& resp : _responses) {
        if (resp.resp.kind() != ShardMessageKind::BLOCK_SERVICE_FILES) {
            continue;
        }
        auto it = _inflightRequests.find(resp.requestId);
        if (it == _inflightRequests.end()) {
            continue;
        }

        auto shardReqStatus = it->second;
        _inflightRequests.erase(it);

        auto bsIt = _blockServiceState.find(shardReqStatus.blockServiceId);
        ALWAYS_ASSERT(bsIt != _blockServiceState.end());

        auto status = bsIt->second;


        auto hasFiles = resp.resp.getBlockServiceFiles().fileIds.els.size() > 0;
        if (hasFiles || status.endShardId == shardReqStatus.shardId){
            _blockServiceState.erase(bsIt);
            if (status.hadFiles != hasFiles) {
                auto& r = reqs.emplace_back();
                r.requestId = 0;
                auto& setFilesReq = r.req.setSetBlockServiceHasFiles();
                setFilesReq.id = shardReqStatus.blockServiceId;
                setFilesReq.hasFiles = hasFiles;
            }
            continue;
        }
        auto requestId = ++_nextRequestId;
        auto shardId = shardReqStatus.shardId == ShardId{255} ? ShardId{0} : ShardId{static_cast<uint8_t>(shardReqStatus.shardId.u8 + 1)};
        _inflightRequests.emplace(requestId, ShardRequestStatus{
            .shardId = shardId,
            .lastSent = 0,
            .blockServiceId = shardReqStatus.blockServiceId
        });
    }
    _writer.pushBlockServiceStateRequests(reqs);
    _responses.clear();
}

void BlockServiceFileTracker::_scanBlockServices() {
    if (_blockServiceState.empty() && _pendingBlockServices.empty()) {
        LOG_INFO(_env, "Scanning for block services to track");
        _registryDB.blockServices(_pendingBlockServices);
    }
    if (_lastRequestBatchTime + 1_sec > ternNow()) {
        return;
    }
    _lastRequestBatchTime = ternNow();
    while(_blockServiceState.size() < MAX_BLOCK_SERVICE_SCAN && !_pendingBlockServices.empty()) {
        auto bsInfo = _pendingBlockServices.back();
        _pendingBlockServices.pop_back();
        _blockServiceState.emplace(bsInfo.id, BlockServiceStatus{
            .hadFiles = false,
            .endShardId = _currentShardIndex == 0 ? ShardId{255} : ShardId{static_cast<uint8_t>(_currentShardIndex.u8 - 1)},
        });
        _inflightRequests.emplace(++_nextRequestId, ShardRequestStatus{
            .shardId = _currentShardIndex,
            .lastSent = 0,
            .blockServiceId = bsInfo.id
        });
        _currentShardIndex = _currentShardIndex == ShardId{255} ? ShardId{0} : ShardId{static_cast<uint8_t>(_currentShardIndex.u8 + 1)};
    }
}

void BlockServiceFileTracker::_sendRequests() {
    TernTime now = ternNow();
    std::vector<std::pair<AddrsInfo, ShardReqMsg>> requestsToSend;

    std::vector<FullShardInfo> cachedShardInfo;
    _registryDB.shards(cachedShardInfo);
    std::array<AddrsInfo, ShardId::SHARD_COUNT> shardAddrs;

    for (const auto& shardInfo : cachedShardInfo) {
        if (shardInfo.locationId != 0 || !shardInfo.isLeader) {
            continue;
        }
        shardAddrs[shardInfo.id.shardId().u8] = shardInfo.addrs;
    }

    for (auto& [requestId, shardReqStatus] : _inflightRequests) {
        if (now - shardReqStatus.lastSent < SHARD_RETRY_TIMEOUT) {
            continue;
        }
        auto& addrInfo = shardAddrs[shardReqStatus.shardId.u8];
        if (addrInfo.addrs[0].port == 0 && addrInfo.addrs[1].port == 0) {
            LOG_DEBUG(_env, "No address info for shard %s, cannot send request", shardReqStatus.shardId);
            continue;
        }

        ShardReqMsg msg;
        msg.id = requestId;
        msg.body.setBlockServiceFiles().blockServiceId = shardReqStatus.blockServiceId;

        requestsToSend.emplace_back(addrInfo, std::move(msg));
        shardReqStatus.lastSent = now;
        LOG_TRACE(_env, "Sending BlockServiceFiles request %s for block service %s shard %s to %s",
            msg.id, shardReqStatus.blockServiceId, shardReqStatus.shardId, addrInfo);
    }

    if (!requestsToSend.empty()) {
        _server.sendShardRequests(requestsToSend);
    }
}

void BlockServiceFileTracker::sendStop() {
    Loop::sendStop();
}
