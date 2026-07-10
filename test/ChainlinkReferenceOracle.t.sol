// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Test } from "forge-std/Test.sol";

import { IChainlinkAggregatorV3 } from "src/interfaces/IChainlinkAggregatorV3.sol";
import { ChainlinkReferenceOracle } from "src/oracles/ChainlinkReferenceOracle.sol";

import { ManualAggregatorV3 } from "./helpers/ManualAggregatorV3.sol";

contract ChainlinkReferenceOracleTest is Test {
    function test_latestPriceWad_returnsBaseQuoteRatioAndOldestTimestamp() public {
        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(8, 2000e8, 200);
        ManualAggregatorV3 quoteFeed = new ManualAggregatorV3(8, 1250e8, 150);
        ChainlinkReferenceOracle oracle =
            new ChainlinkReferenceOracle(baseFeed, false, quoteFeed, false, 18, 18);

        (uint256 priceWad, uint256 updatedAt, uint256 latestFeedTs) = oracle.latestPriceWad();

        assertEq(priceWad, 1.6e18);
        assertEq(updatedAt, 150);
        assertEq(latestFeedTs, 200);
    }

    function test_latestPriceWad_supportsInvertedFeeds() public {
        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(8, 5e7, 100);
        ChainlinkReferenceOracle oracle = new ChainlinkReferenceOracle(
            baseFeed, true, IChainlinkAggregatorV3(address(0)), false, 18, 18
        );

        (uint256 priceWad, uint256 updatedAt, uint256 latestFeedTs) = oracle.latestPriceWad();

        assertEq(priceWad, 2e18);
        assertEq(updatedAt, 100);
        assertEq(latestFeedTs, 100);
    }

    function test_latestPriceWad_revertsOnNonPositiveAnswer() public {
        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(8, 0, 100);
        ChainlinkReferenceOracle oracle = new ChainlinkReferenceOracle(
            baseFeed, false, IChainlinkAggregatorV3(address(0)), false, 18, 18
        );

        vm.expectRevert(
            abi.encodeWithSelector(
                ChainlinkReferenceOracle.InvalidFeedAnswer.selector, address(baseFeed), int256(0)
            )
        );
        oracle.latestPriceWad();
    }

    function test_latestPriceWad_revertsOnIncompleteRound() public {
        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(8, 1e8, 100);
        baseFeed.setRoundDataStatus(2e8, 200, 1);

        ChainlinkReferenceOracle oracle = new ChainlinkReferenceOracle(
            baseFeed, false, IChainlinkAggregatorV3(address(0)), false, 18, 18
        );

        vm.expectRevert(
            abi.encodeWithSelector(
                ChainlinkReferenceOracle.IncompleteRound.selector,
                address(baseFeed),
                uint80(2),
                uint80(1)
            )
        );
        oracle.latestPriceWad();
    }

    function test_constructor_revertsWhenFeedDecimalsExceedWadPrecision() public {
        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(19, 1e18, 100);

        vm.expectRevert(
            abi.encodeWithSelector(
                ChainlinkReferenceOracle.FeedDecimalsTooLarge.selector, uint8(19)
            )
        );
        new ChainlinkReferenceOracle(
            baseFeed, false, IChainlinkAggregatorV3(address(0)), false, 18, 18
        );
    }

    function test_latestPriceWad_normalizesHeterogeneousTokenDecimals() public {
        // USDC/WETH-style pool: token0 = USDC (6 decimals), token1 = WETH (18 decimals),
        // base feed = USDC/USD, quote feed = ETH/USD at $2000. One raw USDC unit
        // (1e-6 USDC) buys 5e-10 ETH = 5e8 wei, so amount1/amount0 in WAD is 5e26.
        ManualAggregatorV3 baseFeed = new ManualAggregatorV3(8, 1e8, 200);
        ManualAggregatorV3 quoteFeed = new ManualAggregatorV3(8, 2000e8, 150);
        ChainlinkReferenceOracle oracle =
            new ChainlinkReferenceOracle(baseFeed, false, quoteFeed, false, 6, 18);

        (uint256 priceWad,, uint256 latestFeedTs) = oracle.latestPriceWad();

        assertEq(priceWad, 5e26);
        assertEq(latestFeedTs, 200);
    }
}
