// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

interface IReferenceOracle {
    /// @notice Returns the reference price as a pool-unit ratio.
    /// @dev `priceWad` is the amount1-per-amount0 price in raw token units, WAD-scaled.
    function latestPriceWad()
        external
        view
        returns (uint256 priceWad, uint256 updatedAt, uint256 latestFeedTs);
}
