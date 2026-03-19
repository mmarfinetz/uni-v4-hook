// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { FullMath } from "v4-core/libraries/FullMath.sol";

import { IChainlinkAggregatorV3 } from "../interfaces/IChainlinkAggregatorV3.sol";
import { IReferenceOracle } from "../interfaces/IReferenceOracle.sol";

contract ChainlinkReferenceOracle is IReferenceOracle {
    uint256 internal constant WAD = 1e18;

    error FeedDecimalsTooLarge(uint8 decimals);
    error IncompleteRound(address feed, uint80 roundId, uint80 answeredInRound);
    error InvalidFeed(address feed);
    error InvalidFeedAnswer(address feed, int256 answer);

    IChainlinkAggregatorV3 public immutable baseFeed;
    IChainlinkAggregatorV3 public immutable quoteFeed;
    bool public immutable hasQuoteFeed;
    bool public immutable invertBase;
    bool public immutable invertQuote;

    uint8 internal immutable baseFeedDecimals;
    uint8 internal immutable quoteFeedDecimals;

    constructor(
        IChainlinkAggregatorV3 _baseFeed,
        bool _invertBase,
        IChainlinkAggregatorV3 _quoteFeed,
        bool _invertQuote
    ) {
        if (address(_baseFeed) == address(0)) revert InvalidFeed(address(0));

        baseFeed = _baseFeed;
        quoteFeed = _quoteFeed;
        hasQuoteFeed = address(_quoteFeed) != address(0);
        invertBase = _invertBase;
        invertQuote = _invertQuote;

        baseFeedDecimals = _validatedDecimals(_baseFeed);
        quoteFeedDecimals = hasQuoteFeed ? _validatedDecimals(_quoteFeed) : 0;
    }

    function latestPriceWad() external view returns (uint256 priceWad, uint256 updatedAt) {
        (uint256 basePriceWad, uint256 baseUpdatedAt) =
            _readFeed(baseFeed, baseFeedDecimals, invertBase);

        if (!hasQuoteFeed) {
            return (basePriceWad, baseUpdatedAt);
        }

        (uint256 quotePriceWad, uint256 quoteUpdatedAt) =
            _readFeed(quoteFeed, quoteFeedDecimals, invertQuote);
        priceWad = FullMath.mulDiv(basePriceWad, WAD, quotePriceWad);
        updatedAt = baseUpdatedAt < quoteUpdatedAt ? baseUpdatedAt : quoteUpdatedAt;
    }

    function _readFeed(IChainlinkAggregatorV3 feed, uint8 decimals, bool invert)
        internal
        view
        returns (uint256 priceWad, uint256 updatedAt)
    {
        (uint80 roundId, int256 answer,, uint256 feedUpdatedAt, uint80 answeredInRound) =
            feed.latestRoundData();
        if (answer <= 0) revert InvalidFeedAnswer(address(feed), answer);
        if (feedUpdatedAt == 0 || answeredInRound < roundId) {
            revert IncompleteRound(address(feed), roundId, answeredInRound);
        }

        priceWad = FullMath.mulDiv(uint256(answer), WAD, 10 ** decimals);
        if (invert) {
            priceWad = FullMath.mulDiv(WAD, WAD, priceWad);
        }

        updatedAt = feedUpdatedAt;
    }

    function _validatedDecimals(IChainlinkAggregatorV3 feed)
        internal
        view
        returns (uint8 decimals)
    {
        decimals = feed.decimals();
        if (decimals > 18) revert FeedDecimalsTooLarge(decimals);
    }
}
