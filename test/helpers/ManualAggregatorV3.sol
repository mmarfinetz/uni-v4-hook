// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { IChainlinkAggregatorV3 } from "../../src/interfaces/IChainlinkAggregatorV3.sol";

contract ManualAggregatorV3 is IChainlinkAggregatorV3 {
    uint8 public immutable decimals;

    int256 internal answer;
    uint80 internal roundId;
    uint256 internal updatedAt;
    uint80 internal answeredInRound;

    constructor(uint8 decimals_, int256 answer_, uint256 updatedAt_) {
        decimals = decimals_;
        roundId = 1;
        answer = answer_;
        updatedAt = updatedAt_;
        answeredInRound = roundId;
    }

    function latestRoundData() external view returns (uint80, int256, uint256, uint256, uint80) {
        return (roundId, answer, 0, updatedAt, answeredInRound);
    }

    function setRoundData(int256 answer_, uint256 updatedAt_) external {
        roundId++;
        answer = answer_;
        updatedAt = updatedAt_;
        answeredInRound = roundId;
    }

    function setRoundDataStatus(int256 answer_, uint256 updatedAt_, uint80 answeredInRound_)
        external
    {
        roundId++;
        answer = answer_;
        updatedAt = updatedAt_;
        answeredInRound = answeredInRound_;
    }
}
