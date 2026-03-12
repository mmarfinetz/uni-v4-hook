// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

interface IReferenceOracle {
    function latestPriceWad() external view returns (uint256 priceWad, uint256 updatedAt);
}
